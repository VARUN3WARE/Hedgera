# Testing

## Local CI (fast)

From the repository root:

```bash
./scripts/ci/run-tests.sh
```

Runs `pytest -m "not slow and not finrl"` (unit/catalog tests, no torch/model weight required). Full suite:

```bash
./scripts/ci/run-tests-full.sh
```

GitHub Actions is intentionally disabled for now; use the scripts above before opening a PR. Details: [scripts/ci/README.md](../scripts/ci/README.md).

## Pytest layout

Configuration: [`pytest.ini`](../pytest.ini) at the repo root.

| Path | Purpose |
|------|---------|
| `backend/tests/unit/` | Fast unit tests (streaming factory, pipeline catalog, stubs) |
| `backend/tests/finrl/` | FinRL / explainability unittest suites (`finrl` + `slow` markers) |
| `backend/tests/integration/` | Integration placeholders (`integration` marker) |
| `backend/tests/fixtures/` | Shared data (e.g. `trade_data_3days.csv`) |

### Markers

```bash
pytest -m "not slow and not finrl"   # default local CI
pytest -m finrl                       # model / SHAP / LIME suites
pytest -m integration
```

### FinRL unittest suites

Run individually (requires dependencies and model artifacts):

```bash
pytest backend/tests/finrl/test_finetuning.py -m finrl
pytest backend/tests/finrl/test_explainability.py -m finrl
pytest backend/tests/finrl/test_finrl_base_model.py -m finrl
```

Or the whole directory:

```bash
pytest backend/tests/finrl -m finrl
```

### Manual dev scripts

One-off scripts (pretrain explainers, sample analysis) live under [`backend/scripts/dev/`](../backend/scripts/dev/). They are not part of the default CI set.

## Integration / pipeline smoke tests

```bash
python -m backend.src.cli --single --quick
```

Expect roughly 10–15 minutes depending on API keys and data.

## Data validation

```bash
python historical_data.py
```

Reset helpers:

```bash
python backend/src/utils/clear_redis.py
python backend/src/utils/clear_mongodb.py
```

## Legacy `unit_tests/`

The old folder only contains a [redirect README](../unit_tests/README.md). Do not add new tests there.
