package io.github.ghkdqhrbals.redisstreamcoordinator.protocol

typealias ReleaseVersion = com.redisstream.protocol.ReleaseVersion
typealias ProtocolSupportStatus = com.redisstream.protocol.ProtocolSupportStatus
typealias CoordinationTimingDefaults = com.redisstream.protocol.CoordinationTimingDefaults
typealias CoordinationVersionSupport = com.redisstream.protocol.CoordinationVersionSupport
typealias SupportedVersionRange = com.redisstream.protocol.SupportedVersionRange
typealias CoordinatorCompatibilityResponse = com.redisstream.protocol.CoordinatorCompatibilityResponse

/**
 * Compatibility facade for existing coordinator-server package users.
 *
 * The source of truth lives in `redisstream-core` so the coordinator server and support modules
 * cannot drift on coordination versions or timing defaults.
 */
object CoordinatorProtocol {
    const val MIN_COORDINATION_VERSION: Int = com.redisstream.protocol.CoordinatorProtocol.MIN_COORDINATION_VERSION
    const val CURRENT_COORDINATION_VERSION: Int = com.redisstream.protocol.CoordinatorProtocol.CURRENT_COORDINATION_VERSION
    const val DEFAULT_COORDINATION_VERSION: Int = com.redisstream.protocol.CoordinatorProtocol.DEFAULT_COORDINATION_VERSION
    const val MAX_COORDINATION_VERSION: Int = com.redisstream.protocol.CoordinatorProtocol.MAX_COORDINATION_VERSION

    val DEFAULT_TIMING: CoordinationTimingDefaults = com.redisstream.protocol.CoordinatorProtocol.DEFAULT_TIMING
    val VERSIONS: List<CoordinationVersionSupport> = com.redisstream.protocol.CoordinatorProtocol.VERSIONS

    fun support(version: Int): Boolean =
        com.redisstream.protocol.CoordinatorProtocol.support(version)

    fun timingDefaults(version: Int = CURRENT_COORDINATION_VERSION): CoordinationTimingDefaults =
        com.redisstream.protocol.CoordinatorProtocol.timingDefaults(version)

    fun compatibility(): CoordinatorCompatibilityResponse =
        com.redisstream.protocol.CoordinatorProtocol.compatibility()
}
