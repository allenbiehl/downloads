FROM mambaorg/micromamba:2.0.2

ENV MAMBA_DOCKERFILE_ACTIVATE=1

COPY --chown=$MAMBA_USER:$MAMBA_USER . /app
WORKDIR /app

RUN micromamba install -y -n base -f env.yaml && \
    pip install --no-cache-dir .
