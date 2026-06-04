package com.redisstream.consumer

typealias RedisStreamReleaseVersion = com.redisstream.protocol.ReleaseVersion
typealias CoordinatorConsumerProtocolStatus = com.redisstream.protocol.ProtocolSupportStatus
typealias CoordinatorConsumerVersionSupport = com.redisstream.protocol.CoordinationVersionSupport

/**
 * Consumer-side facade over the shared coordination protocol contract.
 *
 * The source of truth lives in `redisstream-core`; this type only preserves the existing
 * consumer package API while preventing server/client default drift.
 */
object CoordinatorConsumerProtocol {
    const val MIN_COORDINATION_VERSION: Int = com.redisstream.protocol.CoordinatorProtocol.MIN_COORDINATION_VERSION
    const val CURRENT_COORDINATION_VERSION: Int = com.redisstream.protocol.CoordinatorProtocol.CURRENT_COORDINATION_VERSION
    const val DEFAULT_COORDINATION_VERSION: Int = com.redisstream.protocol.CoordinatorProtocol.DEFAULT_COORDINATION_VERSION
    const val MAX_COORDINATION_VERSION: Int = com.redisstream.protocol.CoordinatorProtocol.MAX_COORDINATION_VERSION

    val DEFAULT_TIMING: com.redisstream.protocol.CoordinationTimingDefaults =
        com.redisstream.protocol.CoordinatorProtocol.DEFAULT_TIMING
    val VERSIONS: List<CoordinatorConsumerVersionSupport> =
        com.redisstream.protocol.CoordinatorProtocol.VERSIONS

    fun supportsCoordinationVersion(version: Int): Boolean =
        com.redisstream.protocol.CoordinatorProtocol.support(version)
}
