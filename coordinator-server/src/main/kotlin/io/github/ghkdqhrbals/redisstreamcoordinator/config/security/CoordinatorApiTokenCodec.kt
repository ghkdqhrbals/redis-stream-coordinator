package io.github.ghkdqhrbals.redisstreamcoordinator.config.security

import io.github.ghkdqhrbals.redisstreamcoordinator.config.CoordinatorProperties
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

internal class CoordinatorApiTokenCodec(
    private val api: CoordinatorProperties.Api,
) {
    fun issue(principal: AuthenticatedPrincipal, now: Instant = Instant.now()): IssuedCoordinatorToken {
        require(!api.tokenTtl.isZero && !api.tokenTtl.isNegative) { "coordinator.api.token-ttl must be positive" }
        require(!principal.username.contains('\n')) { "username cannot contain newline" }
        val expiresAt = now.plus(api.tokenTtl)
        val roleNames = principal.roles.map { it.name }.sorted()
        val payload = listOf(
            TOKEN_VERSION,
            principal.username,
            expiresAt.epochSecond.toString(),
            roleNames.joinToString(","),
        ).joinToString("\n")
        val payloadEncoded = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(payload.toByteArray(StandardCharsets.UTF_8))
        val signatureEncoded = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(hmac(payloadEncoded.toByteArray(StandardCharsets.UTF_8)))
        return IssuedCoordinatorToken(
            token = "$payloadEncoded.$signatureEncoded",
            tokenType = "Bearer",
            expiresAt = expiresAt,
            expiresInSeconds = api.tokenTtl.seconds,
            roles = roleNames,
        )
    }

    fun decode(token: String, now: Instant = Instant.now()): AuthenticatedPrincipal? {
        val parts = token.split('.', limit = 2)
        if (parts.size != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            return null
        }
        val expected = hmac(parts[0].toByteArray(StandardCharsets.UTF_8))
        val actual = runCatching { Base64.getUrlDecoder().decode(parts[1]) }.getOrNull() ?: return null
        if (!MessageDigest.isEqual(expected, actual)) {
            return null
        }
        val payload = runCatching {
            String(Base64.getUrlDecoder().decode(parts[0]), StandardCharsets.UTF_8)
        }.getOrNull() ?: return null
        val fields = payload.split('\n')
        if (fields.size != 4 || fields[0] != TOKEN_VERSION) {
            return null
        }
        val expiresAt = fields[2].toLongOrNull()?.let(Instant::ofEpochSecond) ?: return null
        if (!expiresAt.isAfter(now)) {
            return null
        }
        val roles = fields[3]
            .split(',')
            .filter { it.isNotBlank() }
            .mapNotNull { role -> runCatching { CoordinatorProperties.ApiRole.valueOf(role) }.getOrNull() }
            .toSet()
            .takeIf { it.isNotEmpty() }
            ?: return null
        return AuthenticatedPrincipal(
            username = fields[1],
            roles = roles,
        )
    }

    private fun hmac(payload: ByteArray): ByteArray {
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(tokenSecret().toByteArray(StandardCharsets.UTF_8), "HmacSHA256"))
        return mac.doFinal(payload)
    }

    private fun tokenSecret(): String =
        api.tokenSecret.takeIf { it.isNotBlank() }
            ?: "${api.adminUsername}:${api.adminPassword}:redis-stream-coordinator-token"

    private companion object {
        const val TOKEN_VERSION = "v1"
    }
}

internal data class IssuedCoordinatorToken(
    val token: String,
    val tokenType: String,
    val expiresAt: Instant,
    val expiresInSeconds: Long,
    val roles: List<String>,
)
