package io.github.ghkdqhrbals.redisstreamcoordinator.protocol

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
 * Release lifecycle metadata for one coordinator-module coordination version.
 *
 * The `version` value is carried on heartbeat requests as `protocolVersion`, but it is not
 * heartbeat-specific. It fences compatibility between the coordinator server and support modules.
 */
data class CoordinationVersionSupport(
    val version: Int,
    val status: ProtocolSupportStatus,
    val introducedIn: ReleaseVersion,
    val deprecatedIn: ReleaseVersion?,
    val minimumSupportedUntil: ReleaseVersion,
    val removedIn: ReleaseVersion?,
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
    /**
     * Oldest coordination version accepted by this coordinator artifact.
     */
    const val MIN_COORDINATION_VERSION: Int = 1

    /**
     * Coordination version emitted by support modules built from this artifact line.
     */
    const val CURRENT_COORDINATION_VERSION: Int = 1

    /**
     * Newest coordination version accepted by this coordinator artifact.
     */
    const val MAX_COORDINATION_VERSION: Int = 1

    /**
     * Lifecycle metadata for every coordination version this artifact line documents.
     */
    val VERSIONS: List<CoordinationVersionSupport> = listOf(
        CoordinationVersionSupport(
            version = 1,
            status = ProtocolSupportStatus.ACTIVE,
            introducedIn = ReleaseVersion(0, 1, 0),
            deprecatedIn = null,
            minimumSupportedUntil = ReleaseVersion(1, 0, 0),
            removedIn = null,
        ),
    )

    /**
     * Returns true when a heartbeat request's `protocolVersion` is accepted as a coordination version.
     */
    fun support(version: Int): Boolean =
        version in MIN_COORDINATION_VERSION..MAX_COORDINATION_VERSION

    /**
     * Builds the operator-facing compatibility payload.
     */
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
