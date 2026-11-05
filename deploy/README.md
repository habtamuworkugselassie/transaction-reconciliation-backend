# Deploying to a single droplet

Everything runs on one Ubuntu droplet: Postgres, Redis, Kafka (KRaft), the API, one worker,
and Caddy for TLS. That is six containers, defined in `deploy/docker-compose.prod.yml`.
A 2 vCPU / 4 GB droplet is enough.

The app image is built on the droplet from the checkout and tagged
`habtamuwgs/transaction-reconciliation-service:latest`. That tag exists only locally: the
compose file sets `pull_policy: never` for `api` and `worker`, so compose never looks
for it in a registry.

## 1. DNS

Caddy requests a Let's Encrypt certificate for `DOMAIN`. That name must have an A record
pointing at the droplet **before the first start**, or the certificate request fails and
clients get a TLS error. Check it with `dig +short <DOMAIN>`.

## 2. Prepare the droplet

As root:

```bash
curl -fsSL https://get.docker.com | sh

# Open only ssh and web. Postgres, Redis and Kafka publish no ports.
ufw allow OpenSSH && ufw allow 80/tcp && ufw allow 443/tcp && ufw --force enable

# Kafka and Postgres need some swap on a 4 GB host.
fallocate -l 2G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

git clone https://github.com/habtamuworkugselassie/transaction-reconciliation-backend.git /opt/reconciliation
cd /opt/reconciliation
cp deploy/.env.example deploy/.env && chmod 600 deploy/.env
openssl rand -base64 32   # use as POSTGRES_PASSWORD
nano deploy/.env          # set DOMAIN, ACME_EMAIL, POSTGRES_USER, POSTGRES_PASSWORD
```

## 3. Build the image and start the stack

```bash
cd /opt/reconciliation
docker build -t habtamuwgs/transaction-reconciliation-service:latest .   # Maven runs inside the build; a few minutes
docker compose -f deploy/docker-compose.prod.yml up -d
docker compose -f deploy/docker-compose.prod.yml logs -f api worker      # Flyway migrates on first start
```

## 4. Verify

```bash
curl -sI https://$DOMAIN/swagger-ui/index.html | head -1   # 200 with a valid certificate
curl -s  https://$DOMAIN/health/ready                        # {"status":"UP",...}
curl -s  https://$DOMAIN/metrics | head                      # Prometheus text
curl -s -X POST https://$DOMAIN/v1/imports -H 'Idempotency-Key: smoke-1' \
     -F file=@data/transactions-1000.ndjson                  # 202 {"id":...,"status":"pending"}
```

The upload example uses a local file; create it first with
`scripts/generate-data.sh --records=1000`.

The worker's probes and metrics are reachable only on the compose network:
`docker compose -f deploy/docker-compose.prod.yml exec worker wget -qO- http://127.0.0.1:8081/health/ready`.

## 5. Deploying a change

```bash
cd /opt/reconciliation && git pull --ff-only
docker build -t habtamuwgs/transaction-reconciliation-service:latest .
docker compose -f deploy/docker-compose.prod.yml up -d api worker
```

Flyway applies new migrations when the containers restart, and a migration cannot be
rolled back by redeploying the old image, so back up the database first:
`docker compose -f deploy/docker-compose.prod.yml exec -T postgres sh -c 'pg_dump -U "$POSTGRES_USER" "$POSTGRES_DB"' | gzip > db-$(date +%F).sql.gz`.

Restarting the worker is graceful. It stops consuming, finishes the batches already in
flight and releases its import, and the new worker resumes from the committed checkpoint.

## Where the data lives

| What | Host path | Mounted in |
|---|---|---|
| PostgreSQL data | `/var/lib/reconciliation/postgres` | `postgres` |
| Kafka log | `/var/lib/reconciliation/kafka` | `kafka` |
| Uploaded files | `/var/lib/reconciliation/uploads` | `api` (writes), `worker` (reads) |

These are plain directories, not Docker volumes, so no `docker` command removes them.
Set `DATA_DIR` in `deploy/.env` to move them, but only before the first start.
