package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private val executorService = Executors.newCachedThreadPool(NamedThreadFactory("AsyncHttp2Executor"))
//        private val executorService = Executors.newScheduledThreadPool(1000)

        private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(300, NamedThreadFactory("RetryScheduler"))

        private val semaphore = Semaphore(20000, true)
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rate = 1000, window = Duration.ofSeconds(1))

//    val customDispatcher = Dispatcher(Executors.newFixedThreadPool(10000)).apply {
//        maxRequests = 20_000
//        maxRequestsPerHost = 20_000
//    }

//    private val client = OkHttpClient.Builder().dispatcher(customDispatcher)
//        .callTimeout(20000L, TimeUnit.MILLISECONDS)
//        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
//        .build()

    private val client = OkHttpClient.Builder()
        .callTimeout(20000L, TimeUnit.MILLISECONDS)
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        // Фиксируем факт отправки, используемый тестовым сервисом
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        var attempt = 0
        var delayMillis = 200L
        val maxAttempts = 4

        val request = Request.Builder().apply {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}" +
                    "&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        fun tryCallAsync(currentAttempt: Int, currentDelay: Long) {
            executorService.execute {
                semaphore.acquire()
                try {
                    rateLimiter.tickBlocking()
                    client.newCall(request).enqueue(object : Callback {
                        override fun onFailure(call: Call, e: IOException) {
                            semaphore.release()
                            when (e) {
                                is SocketTimeoutException -> {
                                    logger.error("REQ TIMEOUT!!! [$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                                    paymentESService.update(paymentId) {
                                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                                    }
                                }
                                else -> {
                                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                                    paymentESService.update(paymentId) {
                                        it.logProcessing(false, now(), transactionId, reason = e.message)
                                    }
                                }
                            }

                            if (currentAttempt + 1 < maxAttempts) {
                                logger.warn("Backoff for txId: $transactionId, attempt: ${currentAttempt + 1}, next retry in ${currentDelay}ms")
                                scheduler.schedule({
                                    tryCallAsync(currentAttempt + 1, currentDelay * 2)
                                }, currentDelay, TimeUnit.MILLISECONDS)
                            } else {
                                logger.error("Payment failed for txId: $transactionId after $maxAttempts attempts")
                            }
                        }

                        override fun onResponse(call: Call, response: Response) {
                            semaphore.release()
                            response.use {
                                val body = try {
                                    mapper.readValue(it.body?.string(), ExternalSysResponse::class.java)
                                } catch (e: Exception) {
                                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${it.code}", e)
                                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                                }
                                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                                paymentESService.update(paymentId) {
                                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                                }
                                if (!body.result && currentAttempt + 1 < maxAttempts) {
                                    logger.warn("Backoff for txId: $transactionId, attempt: ${currentAttempt + 1}, next retry in ${currentDelay}ms")
                                    scheduler.schedule({
                                        tryCallAsync(currentAttempt + 1, currentDelay * 2)
                                    }, currentDelay, TimeUnit.MILLISECONDS)
                            }
                            }
                        }
                    })
                } catch (ex: Exception) {
                    semaphore.release()
                    logger.error("Exception in tryCallAsync", ex)
                }
            }
        }

        tryCallAsync(attempt, delayMillis)
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()