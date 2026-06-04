package com.redisstream.consumer

import org.springframework.stereotype.Component

/**
 * Declares default settings for coordinator-managed Redis Stream listener methods on this bean.
 *
 * This annotation intentionally mirrors Spring's listener-container style: put shared runtime
 * settings here and put endpoint identity on [StreamListener]. `streamPrefix` and
 * `consumerGroupName` remain available as class-level defaults for compact single-listener beans.
 *
 * Example:
 *
 * ```
 * @StreamConfiguration(
 *     pollBatchSize = 20,
 *     executor = "ordersConsumerExecutor",
 * )
 * class OrderStreamConsumer {
 *     @StreamListener(
 *         id = "order-consumer-a",
 *         streamPrefix = "orders",
 *         groupId = "order-workers",
 *         concurrency = "4",
 *     )
 *     fun consume(message: ConsumedRedisStreamMessage) {
 *         // process message
 *     }
 * }
 * ```
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
@Component
annotation class StreamConfiguration(
    val streamPrefix: String = "",
    val consumerGroupName: String = "",
    val autoStartup: Boolean = true,
    val executor: String = "",
    val pollBatchSize: Long = 10,
    val pollTimeoutMs: Long = 1_000,
    /**
     * Initial heartbeat period in milliseconds before the coordinator returns its recommended
     * interval. Defaults to the shared coordination version timing.
     */
    val heartbeatIntervalMs: Long = -1,
)

/**
 * Marks a method as the message handler for a coordinator-managed Redis Stream endpoint.
 *
 * The shape follows the important parts of Spring Kafka's `@KafkaListener`: the listener method
 * owns its endpoint identity (`id`, `streamPrefix`, `groupId`) and sets logical member
 * concurrency. `concurrency` creates that many independent coordinator members. The starter
 * derives the base member id from pod IP context and appends a member suffix for each logical
 * member, so every member has a separate heartbeat, Redis consumer name, and assignment state.
 * String-valued attributes are resolved through Spring property placeholders, so values such as
 * `${orders.listener.concurrency:4}` are supported.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
annotation class StreamListener(
    val id: String = "",
    val streamPrefix: String = "",
    val groupId: String = "",
    val concurrency: String = "",
    val autoStartup: String = "",
    val executor: String = "",
    val pollBatchSize: String = "",
    val pollTimeoutMs: String = "",
)
