# keda-scaled-job

A proof of concept that grows a Flink TaskManager's generic ephemeral volume with
[KEDA](https://keda.sh) instead of a custom operator. KEDA's Prometheus scaler decides *when*;
a one-shot Java Job (`flink-disk-resizer`) decides *what* and patches the `FlinkDeployment`
spec directly. The stock Flink Kubernetes Operator does the rest.

```
 mock-prometheus  <--- poll: max(used / capacity) over the job's TM PVCs ---  KEDA ScaledJob
                                                                               | ratio > 0.8
                                                                               v
                                                                     Job: flink-disk-resizer
                                                                       1. GET FlinkDeployment, read the
                                                                          TM ephemeral volume's size
                                                                       2. query used/capacity per PVC
                                                                       3. DiskSizePolicy.decide
                                                                       4. one JSON patch: test resourceVersion,
                                                                          replace storage request, add annotations
                                                                               v
                                                                     Flink operator: spec changed -> stateless
                                                                     upgrade -> TMs recreated, new PVCs at new size
```

Compared with the operator + mutator in this repo:

| | operator + webhook | KEDA ScaledJob |
|---|---|---|
| Detection | own poll loop against Prometheus | KEDA Prometheus trigger |
| Decision | `DiskSizePolicy` | the same `DiskSizePolicy` (`:disk-core`) |
| Write | annotation bump, size injected by the admission webhook | JSON patch of the spec |
| Extra moving parts | JOSDK operator, patched operator image, webhook plugin | KEDA (off the shelf), one Job image |
| Survives `helm upgrade` of the job chart | yes, by design | only while the chart's `volumes` list is unchanged |
| Scope | every labelled FlinkDeployment | one ScaledJob per FlinkDeployment |

## What is in here

| Path | What |
|------|------|
| `src/main/java/.../keda/Resizer.java` | The Job's entry point: fetch, decide, patch, exit. |
| `src/main/java/.../keda/ResizerConfig.java` | Environment -> `SizingConfig`, same `DISK_*` names as the operator. |
| `src/main/java/.../keda/SpecPatch.java` | Builds the RFC 6902 patch. Pure, unit tested. |
| `helm/flink-disk-scaledjob/` | ServiceAccount, Role, RoleBinding and the `ScaledJob`. |
| `Dockerfile` | `eclipse-temurin:21-jre` + the `application` plugin's install layout. |

The resize policy, Prometheus client and spec navigation live in [`disk-core/`](../disk-core)
and are shared with the operator.

## The trigger

```promql
max(
  kubelet_volume_stats_used_bytes{namespace="stream",persistentvolumeclaim=~"flink-disk-job-taskmanager-[0-9]+-[0-9]+-local-storage"}
  / kubelet_volume_stats_capacity_bytes{namespace="stream",persistentvolumeclaim=~"flink-disk-job-taskmanager-[0-9]+-[0-9]+-local-storage"}
)
```

with `threshold` = `activationThreshold` = `0.8`. KEDA's Prometheus scaler needs a single-element
result, hence the `max()`; it activates on `value > activationThreshold` and would run
`ceil(value / threshold)` Jobs, which `maxReplicaCount: 1` caps to one. So: one Job per polling
interval for as long as some TaskManager volume of this deployment is reported over 80%.

The Job is the single decision point. It re-reads everything and is idempotent: the size in the
spec is the policy's floor, so a re-run after a successful resize logs
`target size rounds to the current 2Gi` and exits 0.

## The patch

```json
[
  {"op": "test",    "path": "/metadata/resourceVersion", "value": "4711"},
  {"op": "replace", "path": "/spec/taskManager/podTemplate/spec/volumes/0/ephemeral/volumeClaimTemplate/spec/resources/requests/storage", "value": "2Gi"},
  {"op": "add",     "path": "/metadata/annotations/flink-disk-operator.nadberezny.com~1keda-last-resize-reason", "value": "usage ... >= 80.0%, growing 1Gi -> 2Gi"},
  {"op": "add",     "path": "/metadata/annotations/flink-disk-operator.nadberezny.com~1keda-last-resize-at", "value": "2026-09-22T10:00:00Z"}
]
```

The `test` on `resourceVersion` is an exact optimistic lock: if anything touched the resource
between the GET and the PATCH the API server answers 422, the Job fails, and its `backoffLimit`
re-runs it from a fresh GET.

## Configuration

Container environment (set by the chart):

| Variable | Default | Meaning |
|----------|---------|---------|
| `PROMETHEUS_URL` | *(required)* | Base URL of the Prometheus query API |
| `PROMETHEUS_TIMEOUT_SECONDS` | `10` | Per-query timeout |
| `TARGET_NAMESPACE` | *(required)* | Namespace of the FlinkDeployment |
| `FLINK_DEPLOYMENT` | *(required)* | Name of the FlinkDeployment |
| `DISK_THRESHOLD` | `0.80` | Usage ratio that triggers a resize |
| `DISK_TARGET_FILL` | `0.50` | Where usage should land after a resize |
| `DISK_MIN_SIZE` | `1Gi` | Floor; keep equal to the chart's initial size |
| `DISK_MAX_SIZE` | `20Gi` | Ceiling; over it the Job warns and exits 0 |
| `DISK_SIZE_GRANULARITY` | `1Gi` | New sizes are rounded up to a multiple of this |
| `DRY_RUN` | `false` | Log the patch instead of applying it |
| `LOG_LEVEL` | `info` | log4j level for `com.nadberezny.flink.disk` |

Chart values (`helm/flink-disk-scaledjob/values.yaml`): `flinkDeployment.{name,volumeName}`,
`prometheus.{url,timeoutSeconds}`, `disk.{threshold,targetFill,minSize,maxSize,granularity}`,
`image.*`, `keda.{pollingInterval,successfulJobsHistoryLimit,failedJobsHistoryLimit,backoffLimit,activeDeadlineSeconds}`,
`dryRun`, `logLevel`, `resources`. The release must be installed into the FlinkDeployment's
namespace: the Job runs there and its Role is namespaced.

## Running it in k8s-infra-dev

Everything is wired in [`k8s-infra-dev`](../k8s-infra-dev): `keda.tf` installs KEDA,
`keda_scaled_job.tf` installs this chart against the demo job, `flink_operator.tf` runs the stock
operator, and `flink_job.tf` sets `diskAutoscaler.enabled=false` so the forked operator's
in-process scaler stays out of the picture.

Build and push the two images the stack needs (k3d nodes pull from Docker Hub; the host's local
images are invisible to them):

```bash
./gradlew :mock-prometheus:jar :keda-scaled-job:installDist
```

```bash
docker buildx build --platform linux/arm64 --push -t nadberezny/mock-prometheus:0.2 -f mock-prometheus/Dockerfile .
```

```bash
docker buildx build --platform linux/arm64 --push -t nadberezny/flink-disk-resizer:latest -f keda-scaled-job/Dockerfile .
```

The mock tag must be new: it is pulled `IfNotPresent`, and this PoC needs its `max()` support.
Then:

```bash
cd k8s-infra-dev && terraform init -upgrade && terraform apply
```

### Watching it

```bash
kubectl -n stream get scaledjob flink-disk-job-disk-resizer
```

```bash
kubectl -n stream get jobs -l scaledjob.keda.sh/name=flink-disk-job-disk-resizer -w
```

```bash
kubectl -n stream get flinkdeployment flink-disk-job -o jsonpath='{.spec.taskManager.podTemplate.spec.volumes[0].ephemeral.volumeClaimTemplate.spec.resources.requests.storage}{"\n"}'
```

The seeded mock volume (`used=300Mi, growth=5Mi`) crosses 80% of 1Gi on its own after about
100 s, so the first resize needs no prodding. To force one:

```bash
curl -X POST 'http://mock-prometheus.localtest.me:3080/mock/volumes?namespace=stream&pvc=flink-disk-job-taskmanager-1-1-local-storage&used=950Mi&growth=0'
```

A Job appears within one polling interval, its log ends with
`patched TaskManager volume 'local-storage' to 2Gi`, the JobManager and TaskManager pods are
recreated, and the new TaskManager PVC shows `2Gi`. To demo a second step, tell the mock the
volume is now bigger and fuller:

```bash
curl -X POST 'http://mock-prometheus.localtest.me:3080/mock/volumes?namespace=stream&pvc=flink-disk-job-taskmanager-1-1-local-storage&capacity=2Gi&used=1900Mi&growth=0'
```

Confirm the trigger query by hand:

```bash
curl -s --get --data-urlencode 'query=max(kubelet_volume_stats_used_bytes{namespace="stream"} / kubelet_volume_stats_capacity_bytes{namespace="stream"})' http://mock-prometheus.localtest.me:3080/api/v1/query
```

## Known limitations

- **The mock never forgets a series.** Its capacity does not change when the real volume does,
  so after a resize the trigger stays active and KEDA launches a no-op Job every polling interval
  (exit 0, `target size rounds to the current ...`). A real Prometheus ages the old PVC's series
  out once the pod is gone, or the query can be joined with `kube_persistentvolumeclaim_info`.
- **Same PVC name after the roll.** With `upgradeMode: stateless` the operator recreates the
  whole cluster and the new TaskManager pod reuses `flink-disk-job-taskmanager-1-1`, so the
  ephemeral PVC name is identical. Until the old PVC (owned by the old pod) is garbage collected,
  kubelet reports `PVC ... was not created for pod` and the pod waits in `ContainerCreating` for
  a few seconds.
- **Helm drift is narrow but real.** Helm's three-way merge only rewrites `volumes` if
  something in that list changes in the chart; otherwise the live `2Gi` survives `helm upgrade`.
  Terraform's `helm_release` does not detect the live change.
- **FlinkSessionJob** has no pod template; disk lives in the session cluster's FlinkDeployment,
  which is what a ScaledJob would target. Not covered.
- **One ScaledJob per FlinkDeployment.** No discovery through the `managed` label; that is
  KEDA-idiomatic, and Terraform/Helm can stamp one out per job.
- **Metrics are not filtered by PVC existence.** A stale series for a PVC that no longer exists
  is still fed to the policy. Harmless here because the spec size is the floor, but a production
  version would list the deployment's PVCs and intersect.
