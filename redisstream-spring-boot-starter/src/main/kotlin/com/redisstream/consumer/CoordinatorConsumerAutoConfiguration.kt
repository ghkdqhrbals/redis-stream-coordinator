package com.redisstream.consumer

import com.redisstream.RedisStreamCommandsTemplate
import org.springframework.beans.factory.ListableBeanFactory
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.core.env.Environment
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.context.SmartLifecycle
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean

@AutoConfiguration(afterName = ["org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration"])
class CoordinatorConsumerAutoConfiguration {
    @Bean
    @ConditionalOnBean(RedisConnectionFactory::class)
    @ConditionalOnMissingBean
    fun redisStreamCommandsTemplate(redisConnectionFactory: RedisConnectionFactory): RedisStreamCommandsTemplate =
        RedisStreamCommandsTemplate(redisConnectionFactory)

    @Bean
    @ConditionalOnBean(annotation = [StreamConfiguration::class])
    @ConditionalOnMissingBean(StreamListenerConsumerDefinitions::class)
    fun streamListenerConsumerDefinitions(
        beanFactory: ListableBeanFactory,
        environment: Environment,
    ): StreamListenerConsumerDefinitions =
        buildStreamListenerConsumerDefinitions(beanFactory, environment)

    @Bean
    @ConditionalOnBean(RedisStreamCommandsTemplate::class)
    @ConditionalOnMissingBean
    fun redisStreamReader(
        redisStreamCommandsTemplate: RedisStreamCommandsTemplate,
    ): RedisStreamReader =
        SpringDataRedisStreamReader(
            commands = redisStreamCommandsTemplate,
        )

    @Bean
    @ConditionalOnBean(CoordinatorConsumerProperties::class, RedisStreamMessageHandler::class, RedisStreamReader::class)
    @ConditionalOnMissingBean(name = ["coordinatorManagedConsumer"])
    fun coordinatorManagedConsumer(
        properties: CoordinatorConsumerProperties,
        client: CoordinatorClient,
        beanFactory: ListableBeanFactory,
        handler: RedisStreamMessageHandler,
        reader: RedisStreamReader,
    ): SmartLifecycle =
        createMemberConsumers(
            properties = properties,
            handler = handler,
            reader = reader,
            client = client,
            beanFactory = beanFactory,
        ).let { group ->
            CoordinatorManagedConsumerGroup(
                members = group.members,
                validationMembers = group.validationMembers,
            ).also { it.validateInitialRouting() }
        }

    @Bean(name = ["coordinatorManagedConsumer"])
    @ConditionalOnBean(StreamListenerConsumerDefinitions::class, RedisStreamReader::class)
    @ConditionalOnMissingBean(name = ["coordinatorManagedConsumer"])
    fun streamListenerCoordinatorManagedConsumer(
        definitions: StreamListenerConsumerDefinitions,
        client: CoordinatorClient,
        beanFactory: ListableBeanFactory,
        reader: RedisStreamReader,
    ): SmartLifecycle =
        definitions.definitions
            .map { definition ->
                createMemberConsumers(
                    properties = definition.properties,
                    handler = definition.handler,
                    reader = reader,
                    client = client,
                    beanFactory = beanFactory,
                )
            }
            .let { groups ->
                CoordinatorManagedConsumerGroup(
                    members = groups.flatMap { it.members },
                    validationMembers = groups.flatMap { it.validationMembers },
                )
            }
            .let { group ->
                group.validateInitialRouting()
                group
            }
}

private fun CoordinatorConsumerProperties.executor(beanFactory: ListableBeanFactory): Executor? {
    if (executorBeanName.isBlank()) {
        return null
    }
    return beanFactory.getBean(executorBeanName, Executor::class.java)
}

private fun createMemberConsumers(
    properties: CoordinatorConsumerProperties,
    handler: RedisStreamMessageHandler,
    reader: RedisStreamReader,
    client: CoordinatorClient,
    beanFactory: ListableBeanFactory,
): CoordinatorManagedConsumerGroup {
    val executor = properties.executor(beanFactory)
    val members = (0 until properties.memberCount.coerceAtLeast(1)).map { index ->
        val memberProperties = properties.copyForMember(index)
        val lifecycle = RedisStreamConsumerLifecycle(
            properties = memberProperties,
            reader = reader,
            handler = handler,
            executor = executor,
        )
        CoordinatorManagedConsumer(memberProperties, client, lifecycle)
    }
    return CoordinatorManagedConsumerGroup(
        members = members,
        validationMembers = members.take(1),
    )
}

private class CoordinatorManagedConsumerGroup(
    val members: List<CoordinatorManagedConsumer>,
    val validationMembers: List<CoordinatorManagedConsumer>,
) : SmartLifecycle {
    private val running = AtomicBoolean(false)

    override fun start() {
        if (!running.compareAndSet(false, true)) {
            return
        }
        members.forEach(CoordinatorManagedConsumer::start)
    }

    override fun stop() {
        running.set(false)
        members.forEach(CoordinatorManagedConsumer::stop)
    }

    override fun isRunning(): Boolean = running.get()

    fun validateInitialRouting() {
        require(members.isNotEmpty()) {
            "At least one member is required"
        }
        validationMembers.forEach { it.validateInitialRoutingIfAutoStartup() }
    }
}
