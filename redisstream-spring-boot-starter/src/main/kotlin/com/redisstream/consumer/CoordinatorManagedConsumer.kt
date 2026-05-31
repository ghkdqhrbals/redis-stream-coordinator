package com.redisstream.consumer

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

    /**
     * Starts the managed heartbeat loop that joins the coordinator group and receives assignments.
     */
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

    /**
     * Stops heartbeat polling and optionally sends a final graceful-leave heartbeat.
     */
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

    /**
     * Loads coordinator routing metadata during Spring bean creation so missing groups or shards fail fast.
     */
    @Synchronized
    fun validateInitialRouting(): ProducerRoutingResponse {
        validateLocalConfiguration()
        return client.producerRouting(properties.streamPrefix, properties.consumerGroupName)
            .also {
                CoordinatorRoutingMetadataValidator.validate(
                    streamPrefix = properties.streamPrefix,
                    consumerGroupName = properties.consumerGroupName,
                    metadata = it,
                )
            }
    }

    /**
     * Sends one heartbeat and applies the coordinator response to local shard lifecycle state.
     */
    @Synchronized
    fun pollOnce(): HeartbeatResponse {
        validateLocalConfiguration()

        refreshRevocationProgress()
        val runtimeCapacity = runtimeCapacity()
        val shardProgress = shardProgress()
        val response = client.heartbeat(
            streamPrefix = properties.streamPrefix,
            consumerGroup = properties.consumerGroupName,
            memberId = properties.memberId,
            request = HeartbeatRequest(
                protocolVersion = CoordinatorConsumerProtocol.CURRENT_COORDINATION_VERSION,
                requestId = UUID.randomUUID().toString(),
                memberId = properties.memberId,
                memberName = properties.heartbeatMemberName,
                memberEpoch = memberEpoch,
                rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
                metadataVersion = metadataVersion,
                runtimeConsumerCapacity = runtimeCapacity,
                ownedShards = ownedShards,
                revokingShards = revokingShards.values.toList(),
                shardProgress = shardProgress,
            ),
        )

        apply(response)
        return response
    }

    /**
     * Sends one leave heartbeat with local revoke progress so the coordinator can reassign quickly.
     */
    @Synchronized
    fun leaveOnce(): HeartbeatResponse? {
        validateLocalConfiguration()
        if (memberEpoch == 0L && ownedShards.isEmpty() && revokingShards.isEmpty()) {
            return null
        }
        val shardProgress = shardProgress()

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

        val response = client.heartbeat(
            streamPrefix = properties.streamPrefix,
            consumerGroup = properties.consumerGroupName,
            memberId = properties.memberId,
            request = HeartbeatRequest(
                protocolVersion = CoordinatorConsumerProtocol.CURRENT_COORDINATION_VERSION,
                requestId = UUID.randomUUID().toString(),
                memberId = properties.memberId,
                memberName = properties.heartbeatMemberName,
                memberEpoch = -1,
                rebalanceTimeoutMs = properties.rebalanceTimeout.toMillis(),
                metadataVersion = metadataVersion,
                runtimeConsumerCapacity = RuntimeConsumerCapacity(
                    runtimeMaxConcurrency = properties.runtimeMaxConcurrency,
                    availableConcurrency = 0,
                ),
                ownedShards = ownedShards,
                revokingShards = revokingShards.values.toList(),
                shardProgress = shardProgress,
            ),
        )

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

    private fun validateLocalConfiguration() {
        require(properties.streamPrefix.isNotBlank()) { "CoordinatorConsumerProperties.streamPrefix must be set" }
        require(properties.consumerGroupName.isNotBlank()) { "CoordinatorConsumerProperties.consumerGroupName must be set" }
        require(properties.memberId.isNotBlank()) { "CoordinatorConsumerProperties.memberId must be set" }
        require(!properties.heartbeatInterval.isNegative && !properties.heartbeatInterval.isZero) {
            "CoordinatorConsumerProperties.heartbeatInterval must be positive"
        }
        require(!properties.rebalanceTimeout.isNegative && !properties.rebalanceTimeout.isZero) {
            "CoordinatorConsumerProperties.rebalanceTimeout must be positive"
        }
        require(properties.runtimeMaxConcurrency > 0) {
            "CoordinatorConsumerProperties.runtimeMaxConcurrency must be positive"
        }
        require(properties.redis.pollBatchSize > 0) {
            "CoordinatorConsumerProperties.redis.pollBatchSize must be positive"
        }
        require(!properties.redis.pollTimeout.isNegative && !properties.redis.pollTimeout.isZero) {
            "CoordinatorConsumerProperties.redis.pollTimeout must be positive"
        }
    }

    /**
     * Interprets coordinator heartbeat status and either applies assignments or resets fenced state.
     */
    private fun apply(response: HeartbeatResponse) {
        val context = context(response.groupEpoch, response.assignmentEpoch)
        lastContext = context

        when (response.status) {
            HeartbeatStatus.OK -> applyAssignment(response, context)
            HeartbeatStatus.SYNC_METADATA, HeartbeatStatus.REVOKE_PENDING -> applyDrainOnlyAssignment(response, context)
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

    /**
     * Diffs target assignment against local ownership and invokes assign, pending, and revoke callbacks.
     */
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
            revokingShards = nextRevokingShards
        } else {
            revokingShards = nextRevokingShards.filterValues { it.state != RevokingShardState.REVOKED || it.inFlight != 0 }
        }
        ownedShards = nextAssigned
    }

    /**
     * Applies metadata sync and revoke-pending responses without starting newly assigned shards.
     */
    private fun applyDrainOnlyAssignment(response: HeartbeatResponse, context: CoordinatorConsumerContext) {
        memberEpoch = response.memberEpoch
        metadataVersion = response.metadataVersion
        assignedMaxConcurrency = response.assignedMaxConcurrency

        val keepReading = ownedShards
            .filter { it in response.assignment.assignedShards }
            .toSortedSet()
        val revoked = ownedShards - keepReading
        val pending = (response.assignment.assignedShards + response.assignment.pendingShards - keepReading)
            .toSortedSet()
        if (pending.isNotEmpty()) {
            lifecycle.onPending(pending, context)
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
        }
        revokingShards = nextRevokingShards
        ownedShards = keepReading
    }

    private fun context(groupEpoch: Long, assignmentEpoch: Long): CoordinatorConsumerContext =
        CoordinatorConsumerContext(
            memberId = properties.memberId,
            memberName = properties.heartbeatMemberName,
            assignedMaxConcurrency = assignedMaxConcurrency,
            metadataVersion = metadataVersion,
            groupEpoch = groupEpoch,
            assignmentEpoch = assignmentEpoch,
        )

    /**
     * Collects runtime capacity from the lifecycle implementation and validates the reported bounds.
     */
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
        return reported
    }

    /**
     * Collects per-shard consumption progress from the lifecycle when it supports progress reporting.
     */
    private fun shardProgress(): List<ShardConsumptionProgress> {
        val progress = (lifecycle as? CoordinatorShardProgressProvider)?.shardProgress(lastContext)
            ?: return emptyList()
        val reportableShards = ownedShards + revokingShards.keys
        return progress.filter { it.shard in reportableShards }
    }

    /**
     * Re-runs revoke callbacks for shards that were still draining on a previous heartbeat.
     */
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
