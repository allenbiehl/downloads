# FROM python:3.11

# WORKDIR /app

# COPY ./pyproject.toml pyproject.toml
# COPY ./src src

# RUN pip cache purge && \
#   pip install .


# # WORKDIR /app

# COPY pyproject.toml /app/pyproject.toml
# COPY README.md /app/README.md
# COPY src /app/src

# RUN python3.11 -m pip install .


FROM mambaorg/micromamba:2.0.2

WORKDIR /app/

COPY --chown=$MAMBA_USER:$MAMBA_USER env.yaml /app/env.yaml

RUN micromamba install --yes -f env.yaml && \
    micromamba clean --all --yes

ARG MAMBA_DOCKERFILE_ACTIVATE=1

COPY --chown=$MAMBA_USER:$MAMBA_USER ./src /app/src
COPY --chown=$MAMBA_USER:$MAMBA_USER ./pyproject.toml /app/pyproject.toml
COPY --chown=$MAMBA_USER:$MAMBA_USER ./README.md /app/README.md

RUN pip cache purge
RUN pip install . --no-build-isolation

