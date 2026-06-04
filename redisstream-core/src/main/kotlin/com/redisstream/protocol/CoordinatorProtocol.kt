package com.redisstream.protocol

import java.time.Duration

/**
 * Semantic artifact release that introduced, deprecated, guaranteed, or removed a coordination version.
 */
data class ReleaseVersion(
    val major: Int,
    val minor: Int,
    val patch: Int,
    val qualifier: String? = null,
    val version: String = buildString {
        append("$major.$minor.$patch")
        if (!qualifier.isNullOrBlank()) {
            append("-")
            append(qualifier)
        }
    },
)

/**
 * Lifecycle state for a coordination version supported by the coordinator and companion modules.
 */
enum class ProtocolSupportStatus {
    ACTIVE,
    DEPRECATED,
    REMOVED,
}

/**
 * Versioned timing defaults shared by the coordinator server and companion modules.
 *
 * These values are protocol defaults, not local implementation defaults. A consumer may use
 * the heartbeat interval sent by the coordinator response, but both sides must start from the
 * same versioned contract before the first heartbeat succeeds.
 */
data class CoordinationTimingDefaults(
    val heartbeatInterval: Duration,
    val memberLeaseTtl: Duration,
    val rebalanceTimeout: Duration,
)

/**
 * Release lifecycle metadata for one coordinator-module coordination version.
 */
data class CoordinationVersionSupport(
    val version: Int,
    val status: ProtocolSupportStatus,
    val introducedIn: ReleaseVersion,
    val deprecatedIn: ReleaseVersion?,
    val minimumSupportedUntil: ReleaseVersion,
    val removedIn: ReleaseVersion?,
    val timingDefaults: CoordinationTimingDefaults,
)

/**
 * Inclusive coordination version range accepted by this coordinator artifact.
 */
data class SupportedVersionRange(
    val min: Int,
    val max: Int,
)

/**
 * Public compatibility view returned by monitoring APIs for operators and clients.
 */
data class CoordinatorCompatibilityResponse(
    val currentCoordinationVersion: Int,
    val supportedCoordinationVersions: SupportedVersionRange,
    val coordinationVersions: List<CoordinationVersionSupport>,
)

/**
 * Module-owned coordination version contract for coordinator server and RedisStream support modules.
 *
 * These values are intentionally not YAML-configurable. A running artifact must only advertise
 * versions that its code can actually parse, validate, and enforce.
 */
object CoordinatorProtocol {
    const val MIN_COORDINATION_VERSION: Int = 1
    const val CURRENT_COORDINATION_VERSION: Int = 1
    const val DEFAULT_COORDINATION_VERSION: Int = CURRENT_COORDINATION_VERSION
    const val MAX_COORDINATION_VERSION: Int = 1

    val VERSION_1_TIMING_DEFAULTS: CoordinationTimingDefaults = CoordinationTimingDefaults(
        heartbeatInterval = Duration.ofSeconds(3),
        memberLeaseTtl = Duration.ofSeconds(15),
        rebalanceTimeout = Duration.ofSeconds(60),
    )

    val DEFAULT_TIMING: CoordinationTimingDefaults = VERSION_1_TIMING_DEFAULTS

    val VERSIONS: List<CoordinationVersionSupport> = listOf(
        CoordinationVersionSupport(
            version = 1,
            status = ProtocolSupportStatus.ACTIVE,
            introducedIn = ReleaseVersion(0, 1, 0),
            deprecatedIn = null,
            minimumSupportedUntil = ReleaseVersion(1, 0, 0),
            removedIn = null,
            timingDefaults = VERSION_1_TIMING_DEFAULTS,
        ),
    )

    fun support(version: Int): Boolean =
        version in MIN_COORDINATION_VERSION..MAX_COORDINATION_VERSION

    fun timingDefaults(version: Int = CURRENT_COORDINATION_VERSION): CoordinationTimingDefaults =
        VERSIONS.firstOrNull { it.version == version }?.timingDefaults
            ?: error("Unsupported coordination version: $version")

    fun compatibility(): CoordinatorCompatibilityResponse =
        CoordinatorCompatibilityResponse(
            currentCoordinationVersion = CURRENT_COORDINATION_VERSION,
            supportedCoordinationVersions = SupportedVersionRange(
                min = MIN_COORDINATION_VERSION,
                max = MAX_COORDINATION_VERSION,
            ),
            coordinationVersions = VERSIONS,
        )
}
