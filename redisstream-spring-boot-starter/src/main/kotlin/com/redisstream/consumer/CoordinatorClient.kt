package com.redisstream.consumer

import org.springframework.web.client.RestClient

interface CoordinatorClient {
    /**
     * Sends a consumer heartbeat and receives assignment, fencing, or retry instructions.
     */
    fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse

    /**
     * Fetches producer routing metadata for the coordinator-managed shard count.
     */
    fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse
}

class RestClientCoordinatorClient(
    private val restClient: RestClient,
) : CoordinatorClient {
    /**
     * Calls the coordinator heartbeat endpoint using Spring RestClient.
     */
    override fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse =
        restClient.post()
            .uri(
                "/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/members/{memberId}/heartbeat",
                streamPrefix,
                consumerGroup,
                memberId,
            )
            .body(request)
            .retrieve()
            .body(HeartbeatResponse::class.java)
            ?: error("Coordinator heartbeat response body was empty")

    /**
     * Calls the coordinator producer-routing endpoint using Spring RestClient.
     */
    override fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        restClient.get()
            .uri("/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/producer-routing", streamPrefix, consumerGroup)
            .retrieve()
            .body(ProducerRoutingResponse::class.java)
            ?: error("Coordinator producer routing response body was empty")
}

/**
 * Builds the shared coordinator HTTP client from the single public starter property.
 */
fun coordinatorRestClient(
    coordinatorBaseUrl: String,
    username: String = "",
    password: String = "",
): RestClient {
    val builder = RestClient.builder()
        .baseUrl(coordinatorBaseUrl)
    if (username.isNotBlank() || password.isNotBlank()) {
        builder.defaultHeaders { headers -> headers.setBasicAuth(username, password) }
    }
    return builder.build()
}
