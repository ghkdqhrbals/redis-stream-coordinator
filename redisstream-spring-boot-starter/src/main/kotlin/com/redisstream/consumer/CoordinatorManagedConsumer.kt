package com.redisstream.consumer

import org.springframework.context.SmartLifecycle
import java.time.Duration
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
    private val metrics: CoordinatorConsumerMetrics = NoopCoordinatorConsumerMetrics,
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
        val wasRunning = running.compareAndSet(true, false)
        if (wasRunning) {
            task?.cancel(false)
            task = null
        }
        if (properties.gracefulLeaveOnStop) {
            runCatching { leaveOnce() }
        }
        if (wasRunning) {
            executor?.shutdownNow()
            executor = null
        }
        (lifecycle as? AutoCloseable)?.close()
    }

    override fun isRunning(): Boolean = running.get()

    @Synchronized
    fun pollOnce(): HeartbeatResponse {
        require(properties.streamPrefix.isNotBlank()) { "redis-stream-coordinator.consumer.stream-prefix must be set" }
        require(properties.consumerGroup.isNotBlank()) { "redis-stream-coordinator.consumer.consumer-group must be set" }
        require(CoordinatorConsumerProtocol.supportsHeartbeat(properties.protocolVersion)) {
            "Unsupported heartbeat protocol version ${properties.protocolVersion}; supported range is " +
                "${CoordinatorConsumerProtocol.MIN_HEARTBEAT_VERSION}..${CoordinatorConsumerProtocol.MAX_HEARTBEAT_VERSION}"
        }

        refreshRevocationProgress()
        val startedAt = Instant.now()
        val runtimeCapacity = runtimeCapacity()
        val response = try {
            client.heartbeat(
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
                    runtimeConsumerCapacity = runtimeCapacity,
                    ownedShards = ownedShards,
                    revokingShards = revokingShards.values.toList(),
                ),
            )
        } catch (error: RuntimeException) {
            metrics.recordHeartbeat(HeartbeatStatus.RETRY, Duration.between(startedAt, Instant.now()))
            throw error
        }

        metrics.recordHeartbeat(response.status, Duration.between(startedAt, Instant.now()))
        apply(response)
        return response
    }

    @Synchronized
    fun leaveOnce(): HeartbeatResponse? {
        require(properties.streamPrefix.isNotBlank()) { "redis-stream-coordinator.consumer.stream-prefix must be set" }
        require(properties.consumerGroup.isNotBlank()) { "redis-stream-coordinator.consumer.consumer-group must be set" }
        require(CoordinatorConsumerProtocol.supportsHeartbeat(properties.protocolVersion)) {
            "Unsupported heartbeat protocol version ${properties.protocolVersion}; supported range is " +
                "${CoordinatorConsumerProtocol.MIN_HEARTBEAT_VERSION}..${CoordinatorConsumerProtocol.MAX_HEARTBEAT_VERSION}"
        }
        val startedAt = Instant.now()
        if (memberEpoch == 0L && ownedShards.isEmpty() && revokingShards.isEmpty()) {
            metrics.recordLeave(null, Duration.between(startedAt, Instant.now()))
            return null
        }

        val leavingShards = (ownedShards + revokingShards.keys).toSortedSet()
        if (leavingShards.isNotEmpty()) {
            val completed = lifecycle.onRevoked(leavingShards, lastContext)
            revokingShards = leavingShards.associateWith { shard ->
                if (shard in completed) {
                    RevokingShardReport(shard, RevokingShardState.REVOKED, inFlight = 0, ackedAt = Instant.now())
                } else {
                    RevokingShardReport(shard, RevokingShardState.DRAINING, inFlight = 1)
                }
            }
        }

        val response = try {
            client.heartbeat(
                streamPrefix = properties.streamPrefix,
                consumerGroup = properties.consumerGroup,
                memberId = properties.memberId,
                request = HeartbeatRequest(
                    protocolVersion = properties.protocolVersion,
                    requestId = UUID.randomUUID().toString(),
                    memberId = properties.memberId,
                    memberName = properties.memberName,
                    memberEpoch = -1,
                    rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
                    metadataVersion = metadataVersion,
                    runtimeConsumerCapacity = RuntimeConsumerCapacity(
                        runtimeMaxConcurrency = properties.runtimeMaxConcurrency,
                        availableConcurrency = 0,
                    ),
                    ownedShards = ownedShards,
                    revokingShards = revokingShards.values.toList(),
                ),
            )
        } catch (error: RuntimeException) {
            metrics.recordLeave(HeartbeatStatus.RETRY, Duration.between(startedAt, Instant.now()))
            throw error
        }

        metrics.recordLeave(response.status, Duration.between(startedAt, Instant.now()))
        if (response.status == HeartbeatStatus.OK ||
            response.status == HeartbeatStatus.UNKNOWN_MEMBER_ID ||
            response.status == HeartbeatStatus.FENCED_MEMBER_EPOCH
        ) {
            memberEpoch = 0
            metadataVersion = 0
            ownedShards = emptySet()
            revokingShards = emptyMap()
        }
        return response
    }

    private fun apply(response: HeartbeatResponse) {
        val context = context(response.groupEpoch, response.assignmentEpoch)
        lastContext = context

        when (response.status) {
            HeartbeatStatus.OK -> applyAssignment(response, context)
            HeartbeatStatus.FENCED_MEMBER_EPOCH, HeartbeatStatus.UNKNOWN_MEMBER_ID -> {
                lifecycle.onFenced(context)
                metrics.recordFenced()
                memberEpoch = 0
                metadataVersion = 0
                ownedShards = emptySet()
                revokingShards = emptyMap()
                metrics.recordAssignment(0, 0, 0)
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
        val nextRevokingShards = revokingShards
            .filterValues { it.state != RevokingShardState.REVOKED || it.inFlight != 0 }
            .toMutableMap()
        if (revoked.isNotEmpty()) {
            val completed = lifecycle.onRevoked(revoked.toSortedSet(), context)
            revoked.forEach { shard ->
                if (shard in completed) {
                    nextRevokingShards[shard] =
                        RevokingShardReport(shard, RevokingShardState.REVOKED, inFlight = 0, ackedAt = Instant.now())
                } else {
                    nextRevokingShards[shard] = RevokingShardReport(shard, RevokingShardState.DRAINING, inFlight = 1)
                }
            }
            metrics.recordRevoked(completed.size)
            revokingShards = nextRevokingShards
        } else {
            revokingShards = nextRevokingShards.filterValues { it.state != RevokingShardState.REVOKED || it.inFlight != 0 }
        }
        ownedShards = nextAssigned
        metrics.recordAssignment(
            assignedShards = ownedShards.size,
            pendingShards = response.assignment.pendingShards.size,
            revokingShards = revokingShards.size,
        )
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

    private fun runtimeCapacity(): RuntimeConsumerCapacity {
        val reported = (lifecycle as? CoordinatorRuntimeCapacityProvider)?.runtimeCapacity(lastContext)
            ?: RuntimeConsumerCapacity(
                runtimeMaxConcurrency = properties.runtimeMaxConcurrency,
                availableConcurrency = properties.runtimeMaxConcurrency,
            )
        require(reported.runtimeMaxConcurrency > 0) { "runtimeMaxConcurrency must be positive" }
        require(reported.availableConcurrency >= 0) { "availableConcurrency must not be negative" }
        require(reported.availableConcurrency <= reported.runtimeMaxConcurrency) {
            "availableConcurrency must be less than or equal to runtimeMaxConcurrency"
        }
        metrics.recordRuntimeCapacity(
            runtimeMaxConcurrency = reported.runtimeMaxConcurrency,
            availableConcurrency = reported.availableConcurrency,
            inFlight = reported.runtimeMaxConcurrency - reported.availableConcurrency,
        )
        return reported
    }

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
        metrics.recordRevoked(completed.size)
        revokingShards = revokingShards.mapValues { (shard, report) ->
            if (shard in completed) {
                RevokingShardReport(shard, RevokingShardState.REVOKED, inFlight = 0, ackedAt = Instant.now())
            } else {
                report
            }
        }
    }
}
