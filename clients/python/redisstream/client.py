from __future__ import annotations

from typing import Any
from urllib.parse import quote

from .errors import CoordinatorHttpError
from .models import HeartbeatRequest, HeartbeatResponse, ProducerRoutingResponse


class CoordinatorClient:
    """Sync HTTP client for the coordinator API used by Python producers and consumers."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str | None = None,
        timeout: float = 5.0,
        session: Any | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = session or self._new_requests_session()
        self._bearer_token = bearer_token

    def login(self, username: str, password: str) -> str:
        """Logs in against the coordinator and stores the returned bearer token."""
        response = self._session.post(
            f"{self.base_url}/coord/v1/auth/login",
            json={"username": username, "password": password},
            timeout=self.timeout,
        )
        data = self._json_or_raise(response)
        token = data.get("accessToken") or data.get("token")
        if not token:
            raise CoordinatorHttpError(getattr(response, "status_code", 0), "login response has no token")
        self._bearer_token = str(token)
        return self._bearer_token

    def heartbeat(
        self,
        stream_prefix: str,
        consumer_group: str,
        member_id: str,
        request: HeartbeatRequest,
    ) -> HeartbeatResponse:
        data = self._request(
            "POST",
            "/coord/v1/streams/{stream}/groups/{group}/members/{member}/heartbeat",
            stream=stream_prefix,
            group=consumer_group,
            member=member_id,
            json=request.to_json(),
        )
        return HeartbeatResponse.from_json(data)

    def producer_routing(self, stream_prefix: str, consumer_group: str) -> ProducerRoutingResponse:
        data = self._request(
            "GET",
            "/coord/v1/streams/{stream}/groups/{group}/producer-routing",
            stream=stream_prefix,
            group=consumer_group,
        )
        return ProducerRoutingResponse.from_json(data)

    def _request(self, method: str, path_template: str, **kwargs: Any) -> dict[str, Any]:
        json_body = kwargs.pop("json", None)
        path = path_template.format(**{key: quote(str(value), safe="") for key, value in kwargs.items()})
        response = self._session.request(
            method,
            f"{self.base_url}{path}",
            json=json_body,
            timeout=self.timeout,
            headers=self._headers(),
        )
        return self._json_or_raise(response)

    def _headers(self) -> dict[str, str]:
        if not self._bearer_token:
            return {}
        return {"Authorization": f"Bearer {self._bearer_token}"}

    @staticmethod
    def _json_or_raise(response: Any) -> dict[str, Any]:
        status_code = int(getattr(response, "status_code", 0))
        if status_code >= 400:
            body = getattr(response, "text", "")
            raise CoordinatorHttpError(status_code, body)
        data = response.json()
        if not isinstance(data, dict):
            raise CoordinatorHttpError(status_code, "coordinator response must be a JSON object")
        return data

    @staticmethod
    def _new_requests_session() -> Any:
        import requests

        return requests.Session()

