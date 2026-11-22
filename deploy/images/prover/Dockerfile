# build-proof-image.sh supplies a resolved digest for the validated Ziren 2.0 image.
ARG GPU_IMAGE
FROM ${GPU_IMAGE}
RUN apt-get update && apt-get install -y --no-install-recommends tini ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY fleet-ziren-node /usr/local/bin/fleet-ziren-node
COPY entrypoint.sh /usr/local/bin/fleet-proof-entrypoint
COPY build.json /usr/local/share/fleet-proof-build.json
RUN chmod 755 /usr/local/bin/fleet-proof-entrypoint /usr/local/bin/fleet-ziren-node
ENV FLEET_BACKEND=cuda FLEET_GPU_ENDPOINT=http://127.0.0.1:3000/twirp/ \
    ZIREN_SERVER_PORT=3000 FLEET_HEALTH_ADDR=0.0.0.0:8080 FLEET_EXIT_WITH_PARENT=false
# Data and credentials are explicit bind mounts; the image contains neither.
ENTRYPOINT ["/usr/bin/tini", "-g", "--", "/usr/local/bin/fleet-proof-entrypoint"]
CMD []
