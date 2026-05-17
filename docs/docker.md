# Docker

### Build Images

```bash
# Build all services
docker-compose build

# Build specific service
docker-compose build aegis-pipeline
```

### Run Services

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f aegis-pipeline

# Stop services
docker-compose down
```

### Service ports

| Service | Port | Notes |
|---------|------|--------|
| Redis | 6379 | `aegis-redis` |
| MongoDB | 27017 | `aegis-mongodb` |
| API | 8000 | `aegis-api` — `python main.py` |
| Frontend | 3000 | `aegis-frontend` — set `BACKEND_URL=http://aegis-api:8000` |

### Docker configuration

**File:** `docker-compose.yml`

Services:

1. **redis** — message broker
2. **mongodb** — historical / auth data
3. **aegis-pipeline** — `python -m backend.src.cli --continuous`
4. **aegis-api** — unified FastAPI (trading + auth)
5. **aegis-frontend** — Next.js dashboard

### Health Checks

```bash
# Check Redis
docker exec aegis-redis redis-cli ping

# Check MongoDB
docker exec aegis-mongodb mongosh --eval "db.adminCommand('ping')"

# Check pipeline
docker logs aegis-pipeline --tail 50
```
