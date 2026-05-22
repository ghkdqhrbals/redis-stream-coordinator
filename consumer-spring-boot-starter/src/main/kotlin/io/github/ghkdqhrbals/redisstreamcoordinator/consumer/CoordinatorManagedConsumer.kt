package io.github.ghkdqhrbals.redisstreamcoordinator.consumer

import org.springframework.context.SmartLifecycle
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class CoordinatorManagedConsumer(
    private val properties: CoordinatorConsumerProperties,
    private val client: CoordinatorClient,
    private val lifecycle: CoordinatorShardLifecycle,
) : SmartLifecycle {
    private val running = AtomicBoolean(false)
    private var executor: ScheduledExecutorService? = null
    private var task: ScheduledFuture<*>? = null
    private var memberEpoch: Long = 0
    private var metadataVersion: Long = 0
    private var assignedMaxConcurrency: Int = properties.runtimeMaxConcurrency
    private var ownedShards: Set<CoordinatorShard> = emptySet()
    private var revokingShards: Map<CoordinatorShard, RevokingShardReport> = emptyMap()
    private var lastContext: CoordinatorConsumerContext = context(0, 0)

    override fun start() {
        if (!properties.autoStartup || !running.compareAndSet(false, true)) {
            return
        }
        executor = Executors.newSingleThreadScheduledExecutor { runnable ->
            Thread(runnable, "redis-stream-coordinator-consumer-${properties.memberId}").apply {
                isDaemon = true
            }
        }
        task = executor!!.scheduleWithFixedDelay(
            { runCatching { pollOnce() } },
            0,
            properties.heartbeatInterval.toMillis(),
            TimeUnit.MILLISECONDS,
        )
    }

    override fun stop() {
        if (!running.compareAndSet(true, false)) {
            return
        }
        task?.cancel(true)
        executor?.shutdownNow()
        task = null
        executor = null
    }

    override fun isRunning(): Boolean = running.get()

    fun pollOnce(): HeartbeatResponse {
        require(properties.streamPrefix.isNotBlank()) { "redis-stream-coordinator.consumer.stream-prefix must be set" }
        require(properties.consumerGroup.isNotBlank()) { "redis-stream-coordinator.consumer.consumer-group must be set" }

        refreshRevocationProgress()
        val response = client.heartbeat(
            streamPrefix = properties.streamPrefix,
            consumerGroup = properties.consumerGroup,
            memberId = properties.memberId,
            request = HeartbeatRequest(
                protocolVersion = properties.protocolVersion,
                requestId = UUID.randomUUID().toString(),
                memberId = properties.memberId,
                memberName = properties.memberName,
                memberEpoch = memberEpoch,
                rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
                metadataVersion = metadataVersion,
                runtimeConsumerCapacity = RuntimeConsumerCapacity(
                    runtimeMaxConcurrency = properties.runtimeMaxConcurrency,
                    availableConcurrency = properties.runtimeMaxConcurrency,
                ),
                ownedShards = ownedShards,
                revokingShards = revokingShards.values.toList(),
            ),
        )

        apply(response)
        return response
    }

    private fun apply(response: HeartbeatResponse) {
        val context = context(response.groupEpoch, response.assignmentEpoch)
        lastContext = context

        when (response.status) {
            HeartbeatStatus.OK -> applyAssignment(response, context)
            HeartbeatStatus.FENCED_MEMBER_EPOCH, HeartbeatStatus.UNKNOWN_MEMBER_ID -> {
                lifecycle.onFenced(context)
                memberEpoch = 0
                metadataVersion = 0
                ownedShards = emptySet()
                revokingShards = emptyMap()
            }
            HeartbeatStatus.RETRY -> Unit
            HeartbeatStatus.UNSUPPORTED_PROTOCOL, HeartbeatStatus.INVALID_REQUEST -> {
                error("Coordinator rejected heartbeat with ${response.status}")
            }
        }
    }

    private fun applyAssignment(response: HeartbeatResponse, context: CoordinatorConsumerContext) {
        memberEpoch = response.memberEpoch
        metadataVersion = response.metadataVersion
        assignedMaxConcurrency = response.assignedMaxConcurrency

        val nextAssigned = response.assignment.assignedShards.toSortedSet()
        val newlyAssigned = nextAssigned - ownedShards
        val revoked = ownedShards - nextAssigned
        if (newlyAssigned.isNotEmpty()) {
            lifecycle.onAssigned(newlyAssigned.toSortedSet(), context)
        }
        if (response.assignment.pendingShards.isNotEmpty()) {
            lifecycle.onPending(response.assignment.pendingShards.toSortedSet(), context)
        }
        if (revoked.isNotEmpty()) {
            val completed = lifecycle.onRevoked(revoked.toSortedSet(), context)
            revokingShards = revoked.associateWith { shard ->
                if (shard in completed) {
                    RevokingShardReport(shard, RevokingShardState.REVOKED, inFlight = 0, ackedAt = Instant.now())
                } else {
                    RevokingShardReport(shard, RevokingShardState.DRAINING, inFlight = 1)
                }
            }
        } else {
            revokingShards = revokingShards.filterValues { it.state != RevokingShardState.REVOKED || it.inFlight != 0 }
        }
        ownedShards = nextAssigned
    }

    private fun context(groupEpoch: Long, assignmentEpoch: Long): CoordinatorConsumerContext =
        CoordinatorConsumerContext(
            memberId = properties.memberId,
            memberName = properties.memberName,
            assignedMaxConcurrency = assignedMaxConcurrency,
            metadataVersion = metadataVersion,
            groupEpoch = groupEpoch,
            assignmentEpoch = assignmentEpoch,
        )

    private fun refreshRevocationProgress() {
        val draining = revokingShards
            .filterValues { it.state == RevokingShardState.DRAINING }
            .keys
        if (draining.isEmpty()) {
            return
        }

        val completed = lifecycle.onRevoked(draining.toSortedSet(), lastContext)
        if (completed.isEmpty()) {
            return
        }
        revokingShards = revokingShards.mapValues { (shard, report) ->
            if (shard in completed) {
                RevokingShardReport(shard, RevokingShardState.REVOKED, inFlight = 0, ackedAt = Instant.now())
            } else {
                report
            }
        }
    }
}
