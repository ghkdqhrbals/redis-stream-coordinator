import unittest

from redisstream.client import CoordinatorClient


class FakeResponse:
    def __init__(self, status_code, data, text=""):
        self.status_code = status_code
        self._data = data
        self.text = text

    def json(self):
        return self._data


class FakeSession:
    def __init__(self):
        self.requests = []

    def post(self, url, **kwargs):
        self.requests.append(("POST", url, kwargs))
        return FakeResponse(200, {"accessToken": "token-1"})

    def request(self, method, url, **kwargs):
        self.requests.append((method, url, kwargs))
        if url.endswith("/producer-routing"):
            return FakeResponse(
                200,
                {
                    "streamPrefix": "create-order",
                    "consumerGroup": "demo-workers",
                    "metadataVersion": 1,
                    "shardCount": 1,
                    "streamKeyPattern": "create-order:{shardIndex}",
                    "shards": [{"shardIndex": 0, "streamKey": "create-order:0", "redisSlot": 1}],
                },
            )
        return FakeResponse(
            200,
            {
                "responseTo": "req-1",
                "status": "OK",
                "memberId": "pod-1",
                "memberEpoch": 1,
                "heartbeatIntervalMs": 3000,
                "rebalanceTimeoutMs": 60000,
                "groupEpoch": 1,
                "assignmentEpoch": 1,
                "metadataVersion": 1,
                "assignment": {"assignedShards": [], "pendingShards": [], "metadataVersion": 1},
            },
        )


class CoordinatorClientTest(unittest.TestCase):
    def test_login_stores_bearer_token_and_uses_authorization_header(self):
        session = FakeSession()
        client = CoordinatorClient("https://coordinator.example.com", session=session)

        token = client.login("admin", "secret")
        client.producer_routing("create-order", "demo-workers")

        self.assertEqual(token, "token-1")
        self.assertEqual(
            session.requests[-1],
            (
                "GET",
                "https://coordinator.example.com/coord/v1/streams/create-order/groups/demo-workers/producer-routing",
                {"json": None, "timeout": 5.0, "headers": {"Authorization": "Bearer token-1"}},
            ),
        )

    def test_path_variables_are_url_encoded(self):
        session = FakeSession()
        client = CoordinatorClient("https://coordinator.example.com", bearer_token="token-1", session=session)

        client.producer_routing("orders/a", "workers b")

        self.assertEqual(
            session.requests[-1][1],
            "https://coordinator.example.com/coord/v1/streams/orders%2Fa/groups/workers%20b/producer-routing",
        )


if __name__ == "__main__":
    unittest.main()
