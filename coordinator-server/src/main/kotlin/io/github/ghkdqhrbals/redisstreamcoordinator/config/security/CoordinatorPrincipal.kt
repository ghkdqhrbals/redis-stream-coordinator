package io.github.ghkdqhrbals.redisstreamcoordinator.config.security

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties

internal object AuthRequestAttributes {
    const val PRINCIPAL = "redisStreamCoordinator.auth.principal"
    const val ROLES = "redisStreamCoordinator.auth.roles"
    const val FAILURE = "redisStreamCoordinator.auth.failure"
}

internal data class AuthenticatedPrincipal(
    val username: String,
    val roles: Set<CoordinatorProperties.ApiRole>,
) {
    fun hasRole(requiredRole: CoordinatorProperties.ApiRole): Boolean =
        roles.any { role -> role.grants(requiredRole) }
}

internal fun CoordinatorProperties.ApiRole.grants(requiredRole: CoordinatorProperties.ApiRole): Boolean =
    when (this) {
        CoordinatorProperties.ApiRole.WRITE ->
            requiredRole == CoordinatorProperties.ApiRole.WRITE ||
                requiredRole == CoordinatorProperties.ApiRole.READ
        CoordinatorProperties.ApiRole.READ ->
            requiredRole == CoordinatorProperties.ApiRole.READ
        CoordinatorProperties.ApiRole.MEMBER ->
            requiredRole == CoordinatorProperties.ApiRole.MEMBER
        CoordinatorProperties.ApiRole.ADMIN ->
            true
        CoordinatorProperties.ApiRole.MONITOR ->
            requiredRole == CoordinatorProperties.ApiRole.READ
    }
