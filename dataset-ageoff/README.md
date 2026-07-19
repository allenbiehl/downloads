# Dataset Age-Off

## Local Development

1. Start Minio

```bash
docker compose up minio
```

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
  --start-date "2026-01-01" \
  --period-type month

dataset-ageoff inventory create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-02-01" \
  --period-type month
  
dataset-ageoff inventory create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-03-01" \
  --period-type month

dataset-ageoff inventory create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-04-01" \
  --period-type month

dataset-ageoff inventory create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-05-01" \
  --period-type month

dataset-ageoff inventory create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-06-01" \
  --period-type month

dataset-ageoff inventory create-period \
  --config resources/ageoff-period-config.yaml \
  --start-date "2026-07-01" \
  --period-type month
```
