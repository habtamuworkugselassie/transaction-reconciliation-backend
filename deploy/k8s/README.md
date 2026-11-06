# Kubernetes

Manifests for the two roles of the service. PostgreSQL, Kafka and Redis are not included:
in a cluster they are managed services or operators (for example CloudNativePG and
Strimzi). Point `configmap.yaml` at them.

| File | What |
|---|---|
| `api.yaml` | API Deployment (2 replicas, `PROCESSING_ENABLED=false`), Service, PodDisruptionBudget |
| `worker.yaml` | Worker Deployment (2 replicas, Kafka consumer + scoring pool), no Service |
| `configmap.yaml` | Non-secret configuration shared by both roles |
| `uploads-pvc.yaml` | Shared upload volume; must be `ReadWriteMany` (ADR-004) |

## Deploy

```bash
# 1. the image, in a registry the cluster can pull from
docker buildx build --platform linux/amd64 -t <registry>/transaction-reconciliation-service:<tag> --push .
#    and set images[0].newName/newTag in kustomization.yaml

# 2. credentials (never committed)
kubectl create namespace reconciliation
kubectl -n reconciliation create secret generic reconciliation-db \
  --from-literal=DATABASE_USERNAME=reconciliation \
  --from-literal=DATABASE_PASSWORD="$(openssl rand -base64 32)"
#    with API keys enabled, add API_KEYS (sha256-hex:provider pairs) to a Secret as well

# 3. everything else
kubectl apply -k deploy/k8s
kubectl -n reconciliation rollout status deploy/reconciliation-api deploy/reconciliation-worker
```

Render without applying: `kubectl kustomize deploy/k8s`.

## How the manifests map to the design

- **Probes**: liveness `/health/live` (process only), readiness `/health/ready` (database,
  storage, Kafka, worker state), and a startup probe that allows 3 minutes for Flyway.
- **Shutdown**: `preStop` waits 5 s so the endpoint removal reaches every node before the
  pod stops accepting. Then SIGTERM triggers the application's graceful shutdown (readiness
  DOWN, HTTP drain, in-flight batches commit, imports released, at most
  `SHUTDOWN_GRACE_PERIOD`). `terminationGracePeriodSeconds: 60` leaves room for all of it.
  If SIGKILL comes anyway, the lease expires and another worker resumes the import.
- **CPU isolation**: the scoring pool is sized explicitly (`RISK_WORKER_THREADS`,
  `-XX:ActiveProcessorCount`) to the worker's CPU request, because without a CPU limit the
  JVM would size it to every core on the node. There is deliberately no CPU limit (throttling
  would stretch batch latency); memory limits are set.
- **Scaling**: API replicas are stateless. Worker parallelism is replicas x
  `PROCESSING_MAX_CONCURRENT_IMPORTS`, capped by the Kafka partition count.
- **Hardening**: non-root UID 1000 (the image's user), read-only root filesystem with an
  `emptyDir` for `/tmp`, no capabilities, no privilege escalation, RuntimeDefault seccomp.
