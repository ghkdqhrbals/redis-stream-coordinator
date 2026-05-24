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


def test_compose_coordinator_profile() -> None:
    compose = read("compose.yaml")
    expectations = [
        "coordinator-server:",
        "profiles:",
        "- coordinator",
        "dockerfile: Dockerfile",
        "COORDINATOR_STORE_TYPE: redis",
        "SPRING_DATA_REDIS_CLUSTER_NODES: redis-node-1:7001,redis-node-2:7002,redis-node-3:7003",
        "COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_0_ADVERTISED_HOST: 127.0.0.1",
        "COORDINATOR_REDIS_CLUSTER_NODE_MAPPINGS_0_CONNECT_HOST: redis-node-1",
        "\"8080:8080\"",
    ]
    for expected in expectations:
        if expected not in compose:
            raise AssertionError(f"compose.yaml is missing {expected!r}")


def test_workflow_permissions_and_smoke_test() -> None:
    workflow = read(".github/workflows/docker-image.yml")
    expectations = [
        "workflow_dispatch:",
        "packages: write",
        "docker build -t redis-stream-coordinator/coordinator-server:ci .",
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
    test_compose_coordinator_profile()
    test_workflow_permissions_and_smoke_test()
    test_docs_are_linked()


if __name__ == "__main__":
    main()
