package io.github.ghkdqhrbals.redisstreamcoordinator.config.web

import jakarta.servlet.http.HttpServletRequest

internal fun String.isMutationMethod(): Boolean =
    equals("POST", ignoreCase = true) ||
        equals("PATCH", ignoreCase = true) ||
        equals("PUT", ignoreCase = true) ||
        equals("DELETE", ignoreCase = true)

internal fun HttpServletRequest.apiMetricGroup(): Pair<String, String>? {
    val parts = requestURI.removePrefix("/").split("/")
    if (parts.size >= 6 && parts[0] == "coord" && parts[1] == "v1" && parts[2] == "streams" && parts[4] == "groups") {
        return parts[3] to parts[5]
    }
    if (
        parts.size >= 8 &&
        parts[0] == "coord" &&
        parts[1] == "v1" &&
        parts[2] == "monitoring" &&
        parts[3] == "streams" &&
        parts[5] == "groups"
    ) {
        return parts[4] to parts[6]
    }
    return null
}
