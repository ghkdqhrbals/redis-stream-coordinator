package io.github.ghkdqhrbals.redisstreamcoordinator.consumer

import org.springframework.http.HttpHeaders
import org.springframework.web.client.RestClient

interface CoordinatorClient {
    fun heartbeat(
        streamPrefix: String,
        consumerGroup: String,
        memberId: String,
        request: HeartbeatRequest,
    ): HeartbeatResponse

    fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse
}

class RestClientCoordinatorClient(
    private val restClient: RestClient,
) : CoordinatorClient {
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

    override fun producerRouting(streamPrefix: String, consumerGroup: String): ProducerRoutingResponse =
        restClient.get()
            .uri("/coord/v1/streams/{streamPrefix}/groups/{consumerGroup}/producer-routing", streamPrefix, consumerGroup)
            .retrieve()
            .body(ProducerRoutingResponse::class.java)
            ?: error("Coordinator producer routing response body was empty")
}

fun coordinatorRestClient(properties: CoordinatorConsumerProperties): RestClient {
    val builder = RestClient.builder().baseUrl(properties.coordinatorBaseUrl)
    val username = properties.username
    val password = properties.password
    if (!username.isNullOrBlank() && password != null) {
        builder.defaultHeader(HttpHeaders.AUTHORIZATION, "Basic ${basicAuth(username, password)}")
    }
    return builder.build()
}

private fun basicAuth(username: String, password: String): String =
    java.util.Base64.getEncoder().encodeToString("$username:$password".toByteArray(Charsets.UTF_8))
