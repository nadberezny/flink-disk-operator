# flink-disk-operator

A POC Kubernetes operator that grows the ephemeral disk of Flink TaskManagers before they run out
of it. Built on the [Java Operator SDK](https://javaoperatorsdk.io/), driven by Prometheus disk
metrics, targeting Flink clusters managed by the
[Flink Kubernetes Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/).

Not production grade. Explicitly a proof of concept.

There are two variants in this repo. This README describes the **operator + admission webhook**
one. The **KEDA ScaledJob** one, which needs no custom operator and patches the FlinkDeployment
spec directly, lives in [`keda-scaled-job/`](keda-scaled-job) with its own README; the two share
the resize policy through [`disk-core/`](disk-core).

## How it works

```
                  ┌────────────────────┐   kubelet_volume_stats_*
                  │  mock-prometheus   │◄──── (faked, on demand)
                  └─────────┬──────────┘
                            │ instant queries
                            ▼
  ┌──────────────────────────────────────────┐
  │            flink-disk-operator           │
  │  1. read volume name + size from spec    │
  │  2. load remembered size (ConfigMap)     │
  │  3. decide: usage >= 80% -> grow         │
  │  4. write ConfigMap, bump annotation     │
  └───────────┬──────────────────┬───────────┘
              │ ConfigMap        │ annotation patch (UPDATE)
              │ (desiredSize)    ▼
              │        ┌──────────────────────────────┐
              │        │ Flink operator webhook       │
              └───────►│ + DiskSizeMutator plugin     │
                       │ writes volumes section       │
                       └──────────────┬───────────────┘
                                      ▼
                              FlinkDeployment
                       (TaskManager ephemeral volume)
```

The operator **never writes the volumes section itself**. It records a decision and bumps an
annotation; that annotation bump is what makes the API server issue an `UPDATE`, which runs the
Flink operator's mutating webhook, where our `FlinkResourceMutator` plugin applies the size. Two
consequences worth internalising:

- The operator's only write to the FlinkDeployment is two annotations. Everything else about the
  resource stays owned by whoever wrote it — Helm keeps owning the volumes section it authored.
- A `helm upgrade` that re-applies the chart's original `1Gi` still comes out the other side at
  the size the operator last decided, because the mutator re-injects it on the way in. That is the
  main reason desired size lives in a ConfigMap rather than in an annotation on the resource.

### Resize policy: target headroom

When the fullest TaskManager volume crosses `DISK_THRESHOLD` (80%), the new size is chosen so
*current* usage sits at `DISK_TARGET_FILL` (50%), rounded up to `DISK_SIZE_GRANULARITY` and capped
at `DISK_MAX_SIZE`:

| used | capacity | ratio | new size |
|------|----------|-------|----------|
| 500Mi | 1Gi | 49% | unchanged (below threshold) |
| 900Mi | 1Gi | 88% | 2Gi |
| 7900Mi | 8Gi | 96% | 16Gi |

This converges in one step regardless of how far past the threshold usage got — a fixed `+1Gi` or
multiplicative `x1.5` step would need several resize-and-restart cycles to catch up with a spike.
`DISK_TARGET_FILL` must be below `DISK_THRESHOLD`, or every resize would land straight back over
the line; the operator refuses to start otherwise.

**Sizes only ever grow.** Shrinking would mean destroying a volume that currently holds data.

## Modules

| Path | What it is |
|------|-----------|
| [`operator/`](operator) | The operator. JOSDK reconciler over `FlinkDeployment`. Decides. |
| [`mutator/`](mutator) | Flink plugin for the Flink operator's admission webhook. Applies. |
| [`disk-core/`](disk-core) | Resize policy, Prometheus client, spec navigation. Shared by the operator and the KEDA resizer. |
| [`keda-scaled-job/`](keda-scaled-job) | The KEDA variant: `ScaledJob` chart + the one-shot `flink-disk-resizer` Job. |
| [`contract/`](contract) | The label/annotation/ConfigMap keys the two share. Dependency-free. |
| [`job/`](job) | A deliberately idle Flink streaming job — the workload under test. |
| [`mock-prometheus/`](mock-prometheus) | Stand-in for Prometheus, so disk pressure can be triggered instead of waited for. |
| [`k8s-helm/flink-disk-job/`](k8s-helm/flink-disk-job) | Helm chart for the `FlinkDeployment`. |
| [`k8s-infra-dev/`](k8s-infra-dev) | Terraform for the local k3d cluster and everything on it. |

## Running it locally

Images are built from the repo root and pushed to the registry named in
`k8s-infra-dev/locals.tf` (`nadberezny` on Docker Hub by default; k3d nodes pull from there and
cannot see the host's local images). Each module's `Dockerfile` header names the Gradle task it
expects to have run first, e.g.

```bash
./gradlew :mock-prometheus:jar :keda-scaled-job:installDist :operator:installDist
```

```bash
docker buildx build --platform linux/arm64 --push -t nadberezny/mock-prometheus:0.2 -f mock-prometheus/Dockerfile .
```

Then:

```bash
cd k8s-infra-dev && terraform init && terraform apply
```

`k8s-infra-dev` currently runs the **stock** Flink operator and the KEDA variant; the operator
Deployment in `flink_disk_operator.tf` is commented out. See
[`keda-scaled-job/README.md`](keda-scaled-job/README.md) for the demo loop of that variant.

### Watching the loop

```bash
kubectl -n flink-disk-operator logs -f deploy/flink-disk-operator
```

```bash
kubectl -n stream get configmap flink-disk-state-flink-disk-job -o yaml
```

Push the fake disk usage over the threshold by hand:

```bash
curl -X POST 'http://mock-prometheus.localtest.me:3080/mock/volumes?namespace=stream&pvc=flink-disk-job-taskmanager-1-1-local-storage&used=950Mi&growth=0'
```

`localtest.me` resolves to 127.0.0.1, and k3d maps host port 3080 to the cluster's ingress.

## The state ConfigMap

One per FlinkDeployment, named `flink-disk-state-<deployment>`, in the deployment's namespace,
owned by it (so it is garbage collected with the job). This is what lets the operator restart
without forgetting how much disk it had already granted, and it is the hand-off point to the
mutator plugin.

`desiredSize` is the contract — a Kubernetes quantity like `2Gi`. Everything else is
observability, rewritten on every poll so the ConfigMap doubles as a live view:

```yaml
data:
  desiredSize: "2Gi"            # authoritative; the mutator reads this
  volumeName: "local-storage"
  resizeGeneration: "1"         # mirrored into the annotation to trigger admission
  lastDecisionReason: "usage ... >= 80.0%, growing 1Gi -> 2Gi"
  atMaxSize: "false"
  lastObservedPvc: "flink-disk-job-taskmanager-1-1-local-storage"
  lastObservedUsed: "900Mi"
  lastObservedCapacity: "1Gi"
  lastObservedRatio: "87.9%"
  updatedAt: "2026-08-24T10:00:00Z"
```

Label/annotation keys live in one place:
[`Names.java`](operator/src/main/java/com/nadberezny/flink/disk/Names.java).

## Opting a FlinkDeployment in

Two requirements — the operator ignores anything that does not meet both:

1. the label `flink-disk-operator.nadberezny.com/managed: "true"` (the informer's label selector);
2. a generic ephemeral volume on the TaskManager pod template.

The volume name and current size are read straight out of the spec rather than from annotations,
so the operator always sees what is actually deployed — including whatever the mutator last
injected. See [`deployment.yaml`](k8s-helm/flink-disk-job/templates/deployment.yaml).

Note that the JobManager mounts a plain `emptyDir` at the same path. `flinkConfiguration` is
cluster-wide, so `io.tmp.dirs` applies to the JobManager too, and it fails at startup if that path
does not exist.

## Configuration

All environment variables, set in
[`flink_disk_operator.tf`](k8s-infra-dev/flink_disk_operator.tf):

| Variable | Default | Meaning |
|----------|---------|---------|
| `PROMETHEUS_URL` | *(required)* | Base URL of the Prometheus query API |
| `PROMETHEUS_TIMEOUT_SECONDS` | `10` | Per-query timeout |
| `DISK_THRESHOLD` | `0.80` | Usage ratio that triggers a resize |
| `DISK_TARGET_FILL` | `0.50` | Where usage should land after a resize |
| `DISK_MIN_SIZE` | `1Gi` | Floor for the desired size |
| `DISK_MAX_SIZE` | `20Gi` | Ceiling; over it, the operator warns instead of growing |
| `DISK_SIZE_GRANULARITY` | `1Gi` | New sizes are rounded up to a multiple of this |
| `POLL_INTERVAL_SECONDS` | `30` | Self-scheduled reconcile interval |
| `WATCH_NAMESPACES` | *(all)* | Comma-separated namespace allowlist |
| `LOG_LEVEL` | `info` | `debug` also logs per-poll below-threshold decisions |
| `HEALTH_PORT` | `8080` | `/healthz` and `/readyz` |

## The mutator plugin

[`DiskSizeMutator`](mutator/src/main/java/com/nadberezny/flink/disk/mutator/DiskSizeMutator.java)
implements the Flink operator's
[`FlinkResourceMutator`](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/deployment/plugins/#custom-flink-resource-mutators)
SPI. It ships as a thin jar at `/opt/flink/plugins/flink-disk-mutator/` in a patched
flink-kubernetes-operator image ([`mutator/Dockerfile`](mutator/Dockerfile)) and is picked up by
`MutatorUtils.discoverMutators` via `ServiceLoader`.

**It fails open.** The mutating webhook is `failurePolicy: Fail`, so an exception escaping the
mutator would block every FlinkDeployment create and update in the cluster. Losing a resize is
recoverable — the operator notices on its next poll and bumps the annotation again. Blocking
deploys is not. Like the operator, it only ever grows a volume.

No RBAC changes were needed: the webhook runs as a **sidecar in the operator pod** and shares its
ServiceAccount, which already has full ConfigMap access in the watched namespaces.

### Why the plugin jar must stay thin

This is the part that is easy to get wrong. Flink's plugin classloader
(`ComponentClassLoader`) is *component-only* outside its parent-first patterns — a class that is
neither in the plugin jar nor matched by a pattern is simply **not found**, with no fallback to the
parent. The defaults are just `java.`, `org.apache.flink.` and `javax.annotation.`, which is why
the bundled `flink-metrics-*` plugins are fat jars.

A fat jar is not an option here, though: `FlinkDeployment` is loaded parent-first (it matches
`org.apache.flink.`) and its getters return **parent-loaded fabric8 types**. A bundled second copy
of fabric8 would fail on the first method call. So the plugin has to *share* fabric8 with the
webhook, which needs one line of Flink config:

```
plugin.classloader.parent-first-patterns.additional: io.fabric8.;org.slf4j.
```

That lives in [`mutator/flink-conf-overrides.yaml`](mutator/flink-conf-overrides.yaml), read by
both [`flink_operator.tf`](k8s-infra-dev/flink_operator.tf) (appended to the operator's
`flink-conf.yaml`) and `PluginClassLoadingTest`, so the string cannot drift. Jackson is
deliberately left out — the mutator never touches it directly, and forcing it parent-first would
change classloading for the metrics plugins too.

`PluginClassLoadingTest` loads the real built jar through a real `PluginLoader` with the
patterns the deployed config actually produces, and asserts the jar bundles `:contract` but *not*
fabric8, Flink, slf4j or Jackson.

## mock-prometheus

Serves a small slice of the Prometheus HTTP API over in-memory fake series, plus a control API for
moving the numbers. Supports `kubelet_volume_stats_{used,capacity,available}_bytes`, label matchers
(`=`, `!=`, `=~`, `!~`), a single `/` division between two selectors, and one of
`max|min|sum|count|avg( ... )` wrapped around the whole expression (no `by`/`without`; the result
is a single label-less sample, which is what KEDA's Prometheus scaler requires). Anything else —
other functions, ranges — is rejected with a Prometheus-shaped error rather than quietly
misinterpreted.

```
GET  /api/v1/query?query=<promql>
GET  /metrics                       # also scrapeable by a real Prometheus
GET  /mock/volumes
POST /mock/volumes?namespace=&pvc=&capacity=&used=&growth=&pod=&node=
DELETE /mock/volumes?namespace=&pvc=
POST /mock/reset
```

`growth` is bytes/second of simulated fill, applied lazily at read time, saturating at capacity —
so a seeded volume can walk itself past the threshold with nobody touching it. `MOCK_AUTO_REGISTER`
conjures unknown volumes from `MOCK_AUTO_DEFAULTS` when a query names one exactly, which keeps a
demo alive across TaskManager restarts (each restart mints a fresh ephemeral PVC name).

## Build and test

```bash
./gradlew build
```

Tests cover the parts where being wrong is quiet rather than loud: the resize policy
(`DiskSizePolicyTest`), state serialisation and the PVC-name regex (`DiskStateTest`), spec
navigation (`TaskManagerVolumesTest`), the Prometheus client against a real HTTP server
(`PrometheusClientTest`), the mock's PromQL subset (`PromQlTest`, `QueryEndpointTest`), the KEDA
resizer's JSON patch and configuration (`SpecPatchTest`, `ResizerConfigTest`), mutator behaviour
including fail-open (`DiskSizeMutatorTest`), and plugin classloading against the real built jar
(`PluginClassLoadingTest`).

## Version coupling

Three places pin the Flink operator release and must move together:

| Where | What |
|-------|------|
| `flink_operator_version` in [`locals.tf`](k8s-infra-dev/locals.tf) | The Helm chart version |
| `flinkOperator` in [`libs.versions.toml`](gradle/libs.versions.toml) | The `FlinkResourceMutator` SPI and `FlinkDeployment` model the plugin compiles against |
| `FLINK_OPERATOR_TAG` in [`mutator/Dockerfile`](mutator/Dockerfile) | The base image the plugin is baked into (`79d730b` — the 1.15.0 release commit, which is what the chart's `values.yaml` pins) |

A plugin compiled against a different release than the webhook it runs in will fail at
`ServiceLoader` time, since the SPI is resolved parent-first from the webhook's own jar.

Separately, the Flink operator must be **>= 1.13** for `flinkVersion: v2_1` — 1.12.1's CRD enum
stops at `v2_0`, while the job image is Flink 2.1.0.

## Not done yet

- Real disk pressure. The job is idle by design and disk usage is faked, so the loop is driven by
  the mock rather than by a workload actually filling a volume.
- Leader election. The operator Deployment is pinned to one replica.
- The mutator reads the state ConfigMap synchronously on every admission. Fine at this scale, but a
  shared informer cache would be the production answer.
