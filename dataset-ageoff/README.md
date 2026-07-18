# Dataset Age-Off

## Local Development

1. Install editable mode

```bash
pip install -e .
```

2. Create pending inventory

```bash
dataset-ageoff inventory create-pending \
  --config resources/ageoff-pending-config.yaml
```

2. Create period inventory

```bash
dataset-ageoff inventory create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-07-01" \
  --end-date "2026-07-30 23:59:59"
```
