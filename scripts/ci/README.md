# Local CI

Run checks from the repository root before opening a PR. GitHub Actions is not configured yet to avoid noisy failures while the stack stabilizes.

## Fast check (default)

Matches what a future CI job would run first:

```bash
./scripts/ci/run-tests.sh
```

Installs `pytest` if missing, then runs:

```bash
pytest -m "not slow and not finrl"
```

## Full suite

Includes FinRL / slow tests (torch, model weights, CSV fixture):

```bash
./scripts/ci/run-tests-full.sh
```

## Markers

Defined in [`pytest.ini`](../../pytest.ini):

| Marker | Meaning |
|--------|---------|
| `slow` | Long-running |
| `finrl` | FinRL / torch / model files |
| `integration` | Redis, API, pipeline boundaries |

Examples:

```bash
pytest -m finrl
pytest -m integration
```
