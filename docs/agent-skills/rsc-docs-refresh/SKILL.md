---
name: rsc-docs-refresh
description: Refresh Redis Stream Coordinator docs, release notes, and API/design references for this repository only.
---

# RSC Docs Refresh

Use this project-local skill before changing user-facing docs, PRD pages, release notes, README version snippets, or OpenAPI references.

## Steps

1. Identify the behavior change and the owning docs:
   - API endpoint catalog: `docs/prd/09-api-endpoints.md` and `docs/ko/prd/09-api-endpoints.md`
   - routing and resharding: `docs/prd/04-resharding-routing.md` and Korean counterpart
   - failure modes: `docs/prd/12-failure-modes-edge-cases.md` and Korean counterpart
   - edge-case Q&A: `docs/prd/14-edge-case-qna.md` and Korean counterpart
   - OpenAPI: `docs/openapi/coordinator.v1.yaml`
2. Keep duplicated prose short. Put detailed behavior in the owning doc and link or summarize elsewhere.
3. For releases, add `docs/releases/<version>.md` and `docs/ko/releases/<version>.md`; keep the change list short and scannable.
4. Update README current-version snippets and release links when publishing a new version.
5. Do not include secrets, host credentials, or full operational tokens in docs.

## Verification

Run the narrowest useful verification:

```sh
git diff --check
./gradlew test --no-daemon --rerun-tasks
```
