# Package the unchanged, pinned upstream binary built by the release workflow.
# Use a minimal context containing only this file and openshell-gateway.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl libz3-4 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 10001 fleet \
    && install -d -m 700 -o 10001 -g 10001 /var/lib/openshell /run/zkm-fleet/openshell
COPY --chmod=755 openshell-gateway /usr/local/bin/openshell-gateway
USER 10001:10001
EXPOSE 17670
ENTRYPOINT ["/usr/local/bin/openshell-gateway"]
