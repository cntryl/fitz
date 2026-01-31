# Quick Start - Docker Compose

## Running Fitz with Docker Compose

### Prerequisites
- Docker Desktop or Docker Engine
- Docker Compose v2+

### Start the Broker

```bash
# Build and start
docker compose up -d

# View logs
docker compose logs -f fitz

# Check status
docker compose ps
```

### Access Points

Once running, Fitz is available at:

| Service | URL | Description |
|---------|-----|-------------|
| **SPA** | http://localhost:4090/ | Web interface with live status |
| **Health** | http://localhost:4090/healthz | Liveness probe |
| **Readiness** | http://localhost:4090/readyz | Readiness probe |
| **Metrics** | http://localhost:4090/metrics | Prometheus metrics (requires auth) |
| **Admin API** | http://localhost:4090/api/v1/admin/stats | Broker statistics (requires auth) |
| **WebSocket** | ws://localhost:4090/ws | Data plane (binary protocol) |
| **TCP** | tcp://localhost:4091 | Binary protocol (length-prefixed) |

### Verify It's Working

```bash
# Check health
curl http://localhost:4090/healthz
# Expected: {"status":"ok"}

# Check readiness
curl http://localhost:4090/readyz
# Expected: {"status":"ready","checks":{...}}

# View metrics (with dummy auth token)
curl -H "Authorization: Bearer test" http://localhost:4090/metrics
# Expected: Prometheus format metrics

# Access web UI
open http://localhost:4090/
# Opens browser to SPA landing page
```

### Storage

Data is persisted in a Docker volume:
```bash
# View volume
docker volume ls | grep fitz

# Inspect volume
docker volume inspect fitz_fitz-data

# Backup volume
docker run --rm -v fitz_fitz-data:/data -v $(pwd):/backup ubuntu tar czf /backup/fitz-backup.tar.gz /data

# Restore volume
docker run --rm -v fitz_fitz-data:/data -v $(pwd):/backup ubuntu tar xzf /backup/fitz-backup.tar.gz -C /
```

### Configuration

Edit `compose.yml` to configure:

**Storage Mode:**
```yaml
environment:
  FITZ_STORAGE_MODE: "local"   # local, memory, s3, gcs, azure
  FITZ_STORAGE_PATH: "/data"
```

**Cloud Storage (S3):**
```yaml
environment:
  FITZ_STORAGE_MODE: "s3"
  FITZ_STORAGE_PROVIDER: "s3"
  FITZ_STORAGE_BUCKET: "my-fitz-bucket"
  FITZ_STORAGE_PREFIX: "prod"
  AWS_ACCESS_KEY_ID: "AKIAIOSFODNN7EXAMPLE"
  AWS_SECRET_ACCESS_KEY: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

**Memory-Only (Ephemeral):**
```yaml
environment:
  FITZ_STORAGE_MODE: "memory"
volumes: []  # Remove volume mount
```

### Stopping

```bash
# Stop broker (keeps data)
docker compose stop

# Stop and remove container (keeps data)
docker compose down

# Stop and remove everything (DELETES DATA)
docker compose down -v
```

### Logs

```bash
# Follow logs
docker compose logs -f fitz

# Last 100 lines
docker compose logs --tail=100 fitz

# With timestamps
docker compose logs -t fitz
```

### Troubleshooting

**Container won't start:**
```bash
# Check logs
docker compose logs fitz

# Check if ports are in use
netstat -an | grep 4090
netstat -an | grep 4091

# Rebuild from scratch
docker compose down
docker compose build --no-cache
docker compose up -d
```

**Can't access web UI:**
```bash
# Verify container is running
docker compose ps

# Check if ports are mapped
docker port fitz-node

# Test from inside container
docker compose exec fitz /app/fitz --help  # Won't work (no shell in distroless)

# Check from host
curl -v http://localhost:4090/healthz
```

**Storage issues:**
```bash
# Check volume permissions
docker volume inspect fitz_fitz-data

# Reset storage (WARNING: DELETES DATA)
docker compose down -v
docker compose up -d
```

### Production Deployment

For production, add resource limits:

```yaml
services:
  fitz:
    # ... existing config ...
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
        reservations:
          cpus: '2'
          memory: 4G
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
        window: 120s
```

### Scaling (Future)

For multi-node deployment, use Kubernetes or Docker Swarm:
```bash
# Docker Swarm (not recommended for production)
docker stack deploy -c compose.yml fitz-stack

# Kubernetes (recommended)
# See docs/k8s/ for Kubernetes manifests
```

### Monitoring

**External health checks:**
```bash
# Add to monitoring system
while true; do
  curl -f http://localhost:4090/healthz || echo "UNHEALTHY"
  sleep 10
done
```

**Prometheus scraping:**
```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'fitz'
    static_configs:
      - targets: ['localhost:4090']
    metrics_path: '/metrics'
    bearer_token: 'your-auth-token'
```

### Next Steps

- Read [ADMIN_API.md](docs/ADMIN_API.md) for API documentation
- Review [CLIENT.md](docs/CLIENT.md) for client protocol
- Check [SERVER.md](docs/SERVER.md) for architecture details
- Explore [tests/](tests/) for usage examples
