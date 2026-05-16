# unit_tests (legacy location)

FinRL **pytest** suites and the shared CSV fixture moved to:

- Tests: [`backend/tests/finrl/`](../backend/tests/finrl/)
- Fixture: [`backend/tests/fixtures/trade_data_3days.csv`](../backend/tests/fixtures/trade_data_3days.csv)
- Manual dev scripts: [`backend/scripts/dev/`](../backend/scripts/dev/)

Run fast CI tests from the repo root:

```bash
pytest -m "not slow and not finrl"
```

Run FinRL suites (requires torch, model weights, etc.):

```bash
pytest -m finrl
```
