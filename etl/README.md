# Spark Local

scripts/dev/start_services.sh minio

scripts/dev/start_services.sh etl

docker exec -it etl python -m cdet_etl