#!/usr/bin/env python3

from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def read(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def require_contains(path: str, needle: str) -> None:
    content = read(path)
    if needle not in content:
        raise AssertionError(f"Expected {path} to contain {needle!r}")


def test_dockerfile() -> None:
    dockerfile = read("Dockerfile")
    expectations = [
        "eclipse-temurin:24-jdk",
        "eclipse-temurin:24-jre",
        "./gradlew :coordinator-server:bootJar --no-daemon",
        "! -name \"*-plain.jar\"",
        "USER redisstream",
        "EXPOSE 8080",
        "COORDINATOR_STORE_TYPE=redis",
    ]
    for expected in expectations:
        if expected not in dockerfile:
            raise AssertionError(f"Dockerfile is missing {expected!r}")


def test_compose_pod_stack() -> None:
    compose = read("compose.pods.yaml")
    expectations = [
        "coordinator:",
        "redis-stream-coordinator/coordinator:local",
        "consumer-pod-1:",
        "consumer-pod-2:",
        "publisher-pod:",
        "prometheus:",
        "grafana:",
        "COORDINATOR_STORE_TYPE: redis",
        "SPRING_DATA_REDIS_CLUSTER_NODES: ${AWS_REDIS_CLUSTER_NODES:-}",
        "redis_cluster_nodes:",
        "environment: AWS_REDIS_CLUSTER_NODES",
    ]
    for expected in expectations:
        if expected not in compose:
            raise AssertionError(f"compose.pods.yaml is missing {expected!r}")


def test_compose_stress_stack() -> None:
    compose = read("compose.stress.yaml")
    expectations = [
        "stress-consumer-pod:",
        "stress-publisher-pod:",
        "STREAM_PREFIX: stress-test",
        "CONSUMER_GROUP_NAME: stress-workers",
        "PUBLISHER_XADD_MAX_LEN: \"10000000\"",
        "redis_cluster_nodes:",
        "environment: AWS_REDIS_CLUSTER_NODES",
    ]
    for expected in expectations:
        if expected not in compose:
            raise AssertionError(f"compose.stress.yaml is missing {expected!r}")


def test_workflow_permissions_and_smoke_test() -> None:
    workflow = read(".github/workflows/docker-image.yml")
    expectations = [
        "workflow_dispatch:",
        "packages: write",
        "docker build -t redis-stream-coordinator/coordinator-server:ci .",
        "--tmpfs /tmp:rw,size=128m",
        "COORDINATOR_STORE_TYPE=memory",
        "/coord/v1/monitoring/health",
        "docker/build-push-action@v6",
        "IMAGE: ghcr.io/${{ github.repository }}/coordinator-server",
        "VERSION: ${{ inputs.version }}",
    ]
    for expected in expectations:
        if expected not in workflow:
            raise AssertionError(f"docker-image.yml is missing {expected!r}")


def test_docs_are_linked() -> None:
    require_contains("README.md", "docs/docker.md")
    require_contains("README.md", "CONTRIBUTING.md")
    require_contains("docs/implementation-status.md", "Docker distribution")


def main() -> None:
    test_dockerfile()
    test_compose_pod_stack()
    test_compose_stress_stack()
    test_workflow_permissions_and_smoke_test()
    test_docs_are_linked()


if __name__ == "__main__":
    main()
