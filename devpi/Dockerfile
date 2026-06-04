FROM python:3.11

ENV DEVPI_ROOT=/opt/devpi
ENV DEVPI_SERVER_ROOT=/opt/devpi/server

# Install tools
RUN apt-get update && \
    apt-get install -y \
    vim \
    jq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY ./README.md /build/README.md
COPY ./pyproject.toml /build/pyproject.toml
COPY ./src /build/src
RUN pip install /build

# Setup devpi server
RUN mkdir -p ${DEVPI_SERVER_ROOT} && \
    chmod 770 ${DEVPI_SERVER_ROOT}
WORKDIR ${DEVPI_SERVER_ROOT}

# Setup devpi client
COPY ./resources/devpi/devpi-client /usr/local/bin/devpi-client
RUN chmod 770 /usr/local/bin/devpi-client

# Setup entrypoint
COPY ./resources/entrypoint.sh ${DEVPI_ROOT}/scripts/entrypoint.sh
COPY ./resources/utils.sh ${DEVPI_ROOT}/scripts/utils.sh
ENTRYPOINT [ "/bin/bash", "-c", "${DEVPI_ROOT}/scripts/entrypoint.sh" ]
