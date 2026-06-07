from __future__ import annotations

import os
import socket
import uuid


def default_member_id() -> str:
    """Builds the base member id from pod IP, hostname, then a UUID fallback."""
    pod_ip = os.getenv("POD_IP")
    if pod_ip:
        return pod_ip
    hostname = os.getenv("HOSTNAME") or socket.gethostname()
    if hostname:
        return hostname
    return str(uuid.uuid4())


def split_member_ids(base_member_id: str, concurrency: int) -> list[str]:
    """Creates Kafka-like logical members for one process when concurrency is greater than one."""
    if concurrency < 1:
        raise ValueError("concurrency must be positive")
    if concurrency == 1:
        return [base_member_id]
    return [f"{base_member_id}-m{i}" for i in range(concurrency)]

