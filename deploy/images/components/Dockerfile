# Build context contains only this file and the five release executables.
FROM ubuntu:22.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 fleet \
    && install -d -m 700 -o 10001 -g 10001 /var/lib/zkm-fleet
COPY --chmod=755 fleet-scheduler fleetctl fleet-mcp fleet-node fleet-proof-app /usr/local/bin/
ARG BINARY=fleet-scheduler
RUN test -x "/usr/local/bin/$BINARY" && ln -s "/usr/local/bin/$BINARY" /usr/local/bin/entrypoint
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/entrypoint"]
