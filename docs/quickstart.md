# Quick start

All commands assume the **repository root** as the working directory and a configured `.env` file. See [setup.md](setup.md) first.

## Step 1: Historical data (first run)

Ensure MongoDB has enough bars for FinRL fine-tuning (about three trading days):

```bash
python historical_data.py
```

Expected output includes trading-day counts and `Status: complete`.

## Step 2: Start services

**Redis** (if not using Docker):

```bash
redis-server
redis-cli ping   # PONG
```

**Trading pipeline** (recommended entrypoint):

```bash
python -m backend.src.cli --continuous
```

Other modes:

```bash
python -m backend.src.cli --single --quick   # one fast cycle
python parallel_full_pipeline_clean.py --continuous   # thin shim, same behavior
```

Legacy scripts (`full_pipeline_enhanced.py`, `full_pipeline.py`, …) still exist; see [pipelines.md](pipelines.md).

**API** (monitoring + user auth):

```bash
python main.py
```

Docs: [http://localhost:8000/docs](http://localhost:8000/docs) when running locally.

**Frontend** (optional):

```bash
cd frontend
pnpm install
# Set BACKEND_URL=http://localhost:8000 in .env.local or environment
pnpm dev
```

## Step 3: Monitor

```bash
python monitor_pipeline_detailed.py
```

See [monitoring.md](monitoring.md) for logs and the optional dashboard bridge.

## Step 4: Smoke-test

```bash
./scripts/ci/run-tests.sh
```

See [testing.md](testing.md) for FinRL-heavy suites and markers.
