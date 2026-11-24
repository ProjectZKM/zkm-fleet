# build-worker-image.sh supplies the validated GPU image by digest and the app's worker package.
ARG GPU_IMAGE
FROM ${GPU_IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends tini ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
# The package sits where the entry's worker_command and the worker's own settings name it.
ARG WORKER_PATH
COPY package/ ${WORKER_PATH}/
COPY entrypoint.sh /usr/local/bin/fleet-worker-entrypoint
COPY build.json /usr/local/share/fleet-worker-build.json
RUN chmod 755 /usr/local/bin/fleet-worker-entrypoint
ENV ZIREN_SERVER_PORT=3000
# Data and credentials are explicit bind mounts; the image contains neither. tini signals
# only the entrypoint, which stops the worker before its GPU server.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/fleet-worker-entrypoint"]
CMD []
