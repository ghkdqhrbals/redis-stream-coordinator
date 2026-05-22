package io.github.ghkdqhrbals.redisstreamcoordinator.consumer

object CoordinatorConsumerProtocol {
    const val MIN_HEARTBEAT_VERSION: Int = 1
    const val DEFAULT_HEARTBEAT_VERSION: Int = 1
    const val MAX_HEARTBEAT_VERSION: Int = 1

    fun supportsHeartbeat(version: Int): Boolean =
        version in MIN_HEARTBEAT_VERSION..MAX_HEARTBEAT_VERSION
}
