package com.redisstream.consumer

import org.springframework.beans.factory.ListableBeanFactory
import org.springframework.core.annotation.AnnotationUtils
import org.springframework.core.env.Environment
import java.lang.reflect.Method
import java.time.Duration

internal data class StreamConfigurationDescriptor(
    val annotation: StreamConfiguration,
)

internal data class StreamListenerEndpoint(
    val bean: Any,
    val method: Method,
    val configuration: StreamConfiguration,
    val annotation: StreamListener,
)

data class StreamListenerConsumerDefinition(
    val properties: CoordinatorConsumerProperties,
    val handler: RedisStreamMessageHandler,
)

class StreamListenerConsumerDefinitions(
    val definitions: List<StreamListenerConsumerDefinition>,
) {
    init {
        require(definitions.isNotEmpty()) {
            "At least one @StreamListener method is required when using @StreamConfiguration"
        }
    }
}

internal class StreamListenerMessageHandler(
    private val endpoints: List<StreamListenerEndpoint>,
) : RedisStreamMessageHandler {
    init {
        require(endpoints.isNotEmpty()) {
            "At least one @StreamListener method is required when using @StreamConfiguration"
        }
    }

    override fun handle(message: ConsumedRedisStreamMessage) {
        endpoints.forEach { endpoint ->
            endpoint.method.isAccessible = true
            when (endpoint.method.parameterCount) {
                0 -> endpoint.method.invoke(endpoint.bean)
                1 -> endpoint.method.invoke(endpoint.bean, argument(endpoint.method.parameterTypes[0], message))
                2 -> endpoint.method.invoke(
                    endpoint.bean,
                    argument(endpoint.method.parameterTypes[0], message),
                    argument(endpoint.method.parameterTypes[1], message),
                )
                else -> error("@StreamListener method ${endpoint.method.name} must accept no arguments, ConsumedRedisStreamMessage, RedisStreamAcknowledgement, Map<String, String>, or String")
            }
        }
    }

    private fun argument(type: Class<*>, message: ConsumedRedisStreamMessage): Any =
        when {
            type.isAssignableFrom(ConsumedRedisStreamMessage::class.java) -> message
            type.isAssignableFrom(RedisStreamAcknowledgement::class.java) -> message.acknowledgement
            Map::class.java.isAssignableFrom(type) -> message.fields
            String::class.java.isAssignableFrom(type) -> message.fields["payload"] ?: message.fields.toString()
            else -> error("@StreamListener method parameter must be ConsumedRedisStreamMessage, RedisStreamAcknowledgement, Map<String, String>, or String")
        }
}

internal fun streamConfigurationDescriptors(beanFactory: ListableBeanFactory): List<StreamConfigurationDescriptor> =
    beanFactory.getBeansWithAnnotation(StreamConfiguration::class.java)
        .values
        .mapNotNull { bean ->
            AnnotationUtils.findAnnotation(bean.javaClass, StreamConfiguration::class.java)
                ?.let(::StreamConfigurationDescriptor)
        }

internal fun singleStreamConfigurationDescriptor(beanFactory: ListableBeanFactory): StreamConfigurationDescriptor {
    val descriptors = streamConfigurationDescriptors(beanFactory)
    require(descriptors.isNotEmpty()) { "No @StreamConfiguration bean was found" }
    require(descriptors.size == 1) {
        "Only one @StreamConfiguration bean is supported per application context; found ${descriptors.size}"
    }
    return descriptors.single()
}

internal fun streamListenerEndpoints(beanFactory: ListableBeanFactory): List<StreamListenerEndpoint> =
    beanFactory.getBeansWithAnnotation(StreamConfiguration::class.java)
        .values
        .flatMap { bean ->
            val configuration = requireNotNull(
                AnnotationUtils.findAnnotation(bean.javaClass, StreamConfiguration::class.java),
            ) {
                "@StreamConfiguration metadata is missing for ${bean.javaClass.name}"
            }
            bean.javaClass.methods.mapNotNull { method ->
                AnnotationUtils.findAnnotation(method, StreamListener::class.java)?.let { annotation ->
                    StreamListenerEndpoint(
                        bean = bean,
                        method = method,
                        configuration = configuration,
                        annotation = annotation,
                    )
                }
            }
        }

internal fun buildStreamListenerConsumerDefinitions(
    beanFactory: ListableBeanFactory,
    environment: Environment,
): StreamListenerConsumerDefinitions {
    val endpoints = streamListenerEndpoints(beanFactory)
    require(endpoints.isNotEmpty()) {
        "At least one @StreamListener method is required when using @StreamConfiguration"
    }
    return StreamListenerConsumerDefinitions(
        endpoints.map { endpoint ->
            val properties = CoordinatorConsumerProperties().apply {
                apply(endpoint.configuration, endpoint.annotation, environment)
            }
            StreamListenerConsumerDefinition(
                properties = properties,
                handler = StreamListenerMessageHandler(listOf(endpoint)),
            )
        },
    )
}

internal fun CoordinatorConsumerProperties.apply(
    configuration: StreamConfiguration,
    listener: StreamListener,
    environment: Environment,
) {
    streamPrefix = listener.streamPrefix.resolveRequired(
        fallback = configuration.streamPrefix,
        environment = environment,
        name = "streamPrefix",
    )
    consumerGroupName = listener.groupId.resolveRequired(
        fallback = configuration.consumerGroupName,
        environment = environment,
        name = "groupId",
    )
    autoStartup = listener.autoStartup.resolveBooleanOrNull(environment)
        ?: configuration.autoStartup
    val parsedConcurrency = listener.concurrency.resolveIntOrNull(environment)?.coerceAtLeast(1) ?: 1
    memberCount = parsedConcurrency
    runtimeMaxConcurrency = 1
    executorBeanName = listener.executor.resolveOptional(environment)
        ?: configuration.executor.resolveOptional(environment)
        ?: ""
    redis.pollBatchSize = listener.pollBatchSize.resolveLongOrNull(environment)
        ?.coerceAtLeast(1)
        ?: configuration.pollBatchSize.coerceAtLeast(1)
    val pollTimeoutMs = listener.pollTimeoutMs.resolveLongOrNull(environment)
        ?.coerceAtLeast(1)
        ?: configuration.pollTimeoutMs.coerceAtLeast(1)
    redis.pollTimeout = Duration.ofMillis(pollTimeoutMs)
    val heartbeatIntervalMs = configuration.heartbeatIntervalMs
    if (heartbeatIntervalMs > 0) {
        heartbeatInterval = Duration.ofMillis(heartbeatIntervalMs)
    }
}

private fun String.resolveRequired(
    fallback: String,
    environment: Environment,
    name: String,
): String {
    val value = resolveOptional(environment) ?: fallback.resolveOptional(environment)
    require(!value.isNullOrBlank()) {
        "@StreamListener $name must be set directly or through @StreamConfiguration"
    }
    return value
}

private fun String.resolveOptional(environment: Environment): String? =
    takeIf { it.isNotBlank() }
        ?.let(environment::resolveRequiredPlaceholders)
        ?.trim()
        ?.takeIf { it.isNotBlank() }

private fun String.resolveIntOrNull(environment: Environment): Int? {
    val value = resolveOptional(environment) ?: return null
    return requireNotNull(value.toIntOrNull()) {
        "@StreamListener value '$value' must resolve to an integer"
    }
}

private fun String.resolveLongOrNull(environment: Environment): Long? {
    val value = resolveOptional(environment) ?: return null
    return requireNotNull(value.toLongOrNull()) {
        "@StreamListener value '$value' must resolve to a long integer"
    }
}

private fun String.resolveBooleanOrNull(environment: Environment): Boolean? {
    val value = resolveOptional(environment) ?: return null
    require(value.equals("true", ignoreCase = true) || value.equals("false", ignoreCase = true)) {
        "@StreamListener value '$value' must resolve to true or false"
    }
    return value.equals("true", ignoreCase = true)
}
