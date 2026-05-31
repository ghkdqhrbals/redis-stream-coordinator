package com.redisstream.consumer

data class RedisStreamReleaseVersion(
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

enum class CoordinatorConsumerProtocolStatus {
    ACTIVE,
    DEPRECATED,
    REMOVED,
}

data class CoordinatorConsumerVersionSupport(
    val version: Int,
    val status: CoordinatorConsumerProtocolStatus,
    val introducedIn: RedisStreamReleaseVersion,
    val deprecatedIn: RedisStreamReleaseVersion?,
    val minimumSupportedUntil: RedisStreamReleaseVersion,
    val removedIn: RedisStreamReleaseVersion?,
)

object CoordinatorConsumerProtocol {
    const val MIN_COORDINATION_VERSION: Int = 1
    const val CURRENT_COORDINATION_VERSION: Int = 1
    const val DEFAULT_COORDINATION_VERSION: Int = CURRENT_COORDINATION_VERSION
    const val MAX_COORDINATION_VERSION: Int = 1

    val VERSIONS: List<CoordinatorConsumerVersionSupport> = listOf(
        CoordinatorConsumerVersionSupport(
            version = 1,
            status = CoordinatorConsumerProtocolStatus.ACTIVE,
            introducedIn = RedisStreamReleaseVersion(0, 1, 0),
            deprecatedIn = null,
            minimumSupportedUntil = RedisStreamReleaseVersion(1, 0, 0),
            removedIn = null,
        ),
    )

    fun supportsCoordinationVersion(version: Int): Boolean =
        version in MIN_COORDINATION_VERSION..MAX_COORDINATION_VERSION
}
