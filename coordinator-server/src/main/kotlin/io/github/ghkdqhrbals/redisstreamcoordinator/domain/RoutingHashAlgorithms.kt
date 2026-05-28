package io.github.ghkdqhrbals.redisstreamcoordinator.domain

object RoutingHashAlgorithms {
    const val MURMUR3_32 = "murmur3_32"
    const val MURMUR3_32_UNBIASED = "murmur3_32_unbiased"
    const val DEFAULT = MURMUR3_32_UNBIASED

    fun normalize(value: String?): String =
        when (value?.trim()?.lowercase()) {
            null, "" -> DEFAULT
            "murmur3", "murmur3_32", "murmur3-32" -> MURMUR3_32
            "murmur3_32_unbiased", "murmur3-32-unbiased", "murmur3_unbiased", "murmur3-unbiased" ->
                MURMUR3_32_UNBIASED
            else -> throw IllegalArgumentException("Unsupported hash algorithm $value")
        }
}
