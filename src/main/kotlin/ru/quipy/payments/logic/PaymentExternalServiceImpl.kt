package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
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
    private val semaphore = Semaphore(parallelRequests, true)

    private val rateLimiter = SlidingWindowRateLimiter(rate = rateLimitPerSec.toLong(), window = Duration.ofSeconds(1))

    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val startTimestampMilliseconds = System.currentTimeMillis()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
//        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()


        // Варианты:
        // 1) экспоненц. задержка перед retry
        // делаем три ретрая: (fail) 1retry (fail) 2retry (fail) 3retry (success)
        // 0,7 * 4 = 2,8
        // 3,5 - 2,8 = 0,7
        // 0.7 / 3 retry = 100ms 200ms 400ms
        // Результат: тесты прошли, но не выгодно, т.к. 1 успешный запрос ~= 800, 1 ретрай = 30,
        // отсюда: 800 прибыли на 30*3 = 90 убытка, получаем соотношение прибыли к убытку ~ 88,9 на 11,1

        // 2) Берем соотношение прибыли к убытку для двух ретраев, получаем 800 на 60, что примерно 93,03 на 6,97
        // Тогда заранее знаем, что нам выгодно делать два ретрая, меджу ними три запроса.
        // Считаем время:
        // Запросы: 0,7 * 3 = 2,1
        // Осталось на ретраи: 3,5 - 2,1 = 1,4. Т.к. время обработки одной попытки транзакции примерное, округлим 1,4 до 1,
        // отсюда значение для фикс. времени между бэкоффами 1 / 2 = 500мс

        var attempt = 0
        var delayMillis = 500L
        val maxAttempts = 2

        while (attempt < maxAttempts) {
            semaphore.acquire()
            try {
                rateLimiter.tickBlocking() // case 2
                client.newCall(request).execute().use { response ->
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
                    if (body.result) {
                        return
                    }
                }

            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
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
            } finally {
                semaphore.release()
            }

            attempt++
            if (attempt < maxAttempts) {
                logger.warn("Backoff for txId: $transactionId, attempt: $attempt, next retry in ${delayMillis}ms")
                Thread.sleep(delayMillis)
//                delayMillis *= 2
            } else {
                logger.error("Payment failed for txId: $transactionId after $maxAttempts attempts")
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()