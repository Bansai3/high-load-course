package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(600, Duration.ofSeconds(60))
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)
    private val semaphore = Semaphore(properties.parallelRequests)
    private val retryStatusCodes = listOf(429, 500, 502, 503, 504)
    private val retryLimitAmount = 8


    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        semaphore.acquire()

//        slidingWindowRateLimiter.tickBlocking()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()


        var retryCount = 0
        var delayTime = 1
        val retryAfterHeader = "Retry-After"

        try {

            if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                }
                return
            }
//            Добавляем троттлинг для запросов, по которым не укладываемся в SLA === Очистка очереди
            while (retryCount < retryLimitAmount) {
                while (!slidingWindowRateLimiter.tick()) {
//                    throw ResponseStatusException(
//                    HttpStatus.TOO_MANY_REQUESTS, "Too many requests, please try again later.")
                }
                while (ongoingWindow.putIntoWindow() is NonBlockingOngoingWindow.WindowResponse.Fail) {
                    // Intentionally empty
                }

                val response = client.newCall(request).execute()
                val responseCode = response.code
                val responseHeaders = response.headers
                val retryAfterHeaderTime = responseHeaders[retryAfterHeader]

                if (retryAfterHeaderTime != null) {
                    val retryAfterHeaderTimeValue = retryAfterHeaderTime.toLong()
                    Thread.sleep(retryAfterHeaderTimeValue * 50)
                    retryCount++
                    ongoingWindow.releaseWindow()
                    continue
                }

                if (responseCode in retryStatusCodes) {
                    Thread.sleep((delayTime.toLong() * 50))
                    retryCount++
                    delayTime = if (delayTime == 0) 1 else delayTime * 2
                    ongoingWindow.releaseWindow()
                    continue
                }

                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
                break
            }
        } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error(
                            "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                            e
                        )
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error(
                            "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                            e
                        )

                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = e.message)
                        }
                    }
                }
            } finally {
                semaphore.release()
                if (retryCount < retryLimitAmount) {
                    ongoingWindow.releaseWindow()
                }
            }
        }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()