FROM python:3.11

WORKDIR /app

COPY ./pyproject.toml pyproject.toml
COPY ./src src

RUN pip cache purge && \
  pip install .