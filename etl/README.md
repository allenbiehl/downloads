# Spark Local


## Run locally with Docker

1. Start mino

1.1. Open new terminal

1.2. Start service

```bash
scripts/dev/start_docker_services.sh --profile minio
```

2. Start kafka

4.1. Open new terminal

4.2. Start service

```bash
scripts/dev/start_podman_services.sh --profile kafka
```

3. Start el

3.1. Open new terminal

3.2. Start service

```bash
scripts/dev/start_docker_services.sh --profile etl --build
```

4. Run job

4.1. Open new terminal

4.2. Run job

```bash
docker exec -it etl python -m cdet_etl
```

## Run locally with Podman

1. Init podman

```bash
podman machine init
```

2. Start podman

```bash
podman machine start
```

3. Install zscaler ca

```bash
scripts/dev/install_zscaler_ca.sh
```

4. Start mino

4.1. Open new terminal

4.2. Start service

```bash
scripts/dev/start_podman_services.sh --profile minio
```

5. Start kafka

4.1. Open new terminal

4.2. Start service

```bash
scripts/dev/start_podman_services.sh --profile kafka
```

6. Start el

6.1. Open new terminal

6.2. Start service

```bash
scripts/dev/start_podman_services.sh --profile etl --build
```

7. Run job

7.1. Open new terminal

7.2. Run job

```bash
podman exec -it etl /opt/conda/bin/python -m cdet_etl
```
