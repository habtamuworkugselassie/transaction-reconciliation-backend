# Deploying to a single droplet

Everything runs on one Ubuntu droplet: Postgres, Redis, Kafka (KRaft), the API, one worker,
and Caddy for TLS. That is six containers, defined in `deploy/docker-compose.prod.yml`.
A 2 vCPU / 4 GB droplet is enough.

**Deploys are done by GitHub Actions** (`.github/workflows/deploy.yml`), on every push to
`main` or by hand from the Actions tab:

1. **build** builds the image with the Dockerfile (Maven runs inside the build, so the
   droplet never runs it). It pushes `ghcr.io/habtamuworkugselassie/transaction-reconciliation-backend`
   tagged `:latest` and `:<commit sha>`.
2. **deploy** connects to the droplet over SSH with a read-only, short-lived registry token.
   There it runs `git pull --ff-only`, keeps the running image as a rollback copy, pulls the
   new one and retags it to `habtamuwgs/transaction-reconciliation-service:latest`, then
   runs `up -d api worker caddy` and reloads Caddy. It then waits up to 180 s for
   `/health/ready` on the API and the worker. If either does not come up, it rolls both
   back to the previous image and the run fails.

The retagged name is what `deploy/docker-compose.prod.yml` runs. It exists only on the
droplet: the compose file sets `pull_policy: never` for `api` and `worker`, so compose never
looks for it in a registry.

## 1. Hostname or IP

`DOMAIN` in `deploy/.env` decides how Caddy serves the API:

| `DOMAIN` | Result |
|---|---|
| `api.example.com` | HTTPS with a Let's Encrypt certificate; http redirects to https. The name needs an A record pointing at the droplet **before the first start**, or the certificate request fails and clients get a TLS error. Check with `dig +short api.example.com`. |
| `http://165.245.247.93` | plain HTTP, no certificate. Use this when there is no DNS name: a bare IP gets no Let's Encrypt certificate here, and without the `http://` prefix every request fails the TLS handshake. |

**Ports.** Caddy is published on host ports `HTTP_PORT` (default **9001**) and `HTTPS_PORT`
(default 9443), because 80 and 443 on this droplet belong to another service. The
deployment is therefore at **http://165.245.247.93:9001**. A hostname `DOMAIN` with a Let's
Encrypt certificate needs the standard ports (the certificate challenge arrives on 80): set
`HTTP_PORT=80` and `HTTPS_PORT=443` in `deploy/.env`, if they are free.

Open the port in the firewall, once, as root:

```bash
ufw allow 9001/tcp
ufw status | grep 9001
```

If the droplet also has a DigitalOcean Cloud Firewall (Networking → Firewalls), add an
inbound TCP 9001 rule there too; `ufw` alone is not enough in that case. Every deploy
ends by fetching `/health/ready` from GitHub's network, so a closed port turns the run red.

To change it later, edit `deploy/.env` and recreate Caddy so it picks up the new value:
`docker compose -f deploy/docker-compose.prod.yml up -d --force-recreate caddy`.

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

The workflow expects the checkout at `/opt/reconciliation`. If you clone it somewhere
else, set the Actions variable `DEPLOY_DIR`.

## 3. First deploy

Run **Build and Deploy** from the Actions tab (or push to `main`). The first run also
starts Postgres, Redis and Kafka (the API and worker depend on them) and Caddy. Flyway
creates the schema when the API starts. To watch it on the droplet:

```bash
docker compose -f /opt/reconciliation/deploy/docker-compose.prod.yml logs -f api worker
```

**Without GitHub Actions**, build the image on the droplet under the name compose runs,
and start the stack:

```bash
cd /opt/reconciliation
docker build -t habtamuwgs/transaction-reconciliation-service:latest .   # Maven runs inside the build; a few minutes
docker compose -f deploy/docker-compose.prod.yml up -d
```

## 4. Verify

`BASE` is `https://<hostname>` or `http://<ip>:<HTTP_PORT>`, matching `DOMAIN` and the ports:

```bash
BASE=http://165.245.247.93:9001
curl -sI $BASE/swagger-ui/index.html | head -1   # 200
curl -s  $BASE/health/ready                        # {"status":"UP",...}
curl -s  $BASE/metrics | head                      # Prometheus text
curl -s -X POST $BASE/v1/imports -H 'Idempotency-Key: smoke-1' \
     -F file=@data/transactions-1000.ndjson        # 202 {"id":...,"status":"pending"}
```

The upload example uses a local file; create it first with
`scripts/generate-data.sh --records=1000`.

The worker's probes and metrics are reachable only on the compose network:
`docker compose -f deploy/docker-compose.prod.yml exec worker wget -qO- http://127.0.0.1:8081/health/ready`.

## 5. Deploying a change

Push to `main`. Changes to `deploy/docker-compose.prod.yml` and the `Caddyfile` ship the
same way, because the deploy pulls the checkout before starting anything. A change to
`deploy/.env` is made on the droplet; the next deploy picks it up (or run
`docker compose -f deploy/docker-compose.prod.yml up -d` there).

**Rollback** is automatic when the new image does not become ready. To go back to an
older build by hand, re-run that commit's **Build and Deploy** run from the Actions tab.
Or, on the droplet:

```bash
docker pull ghcr.io/habtamuworkugselassie/transaction-reconciliation-backend:<sha>
docker tag  ghcr.io/habtamuworkugselassie/transaction-reconciliation-backend:<sha> habtamuwgs/transaction-reconciliation-service:latest
docker compose -f deploy/docker-compose.prod.yml up -d api worker
```

Pulling by hand needs `docker login ghcr.io` unless the package is public (GitHub →
Packages → package settings).

**Migrations are not rolled back.** Flyway applies new migrations when the API restarts,
and redeploying the old image does not undo them. Back up the database before merging a
migration:
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
