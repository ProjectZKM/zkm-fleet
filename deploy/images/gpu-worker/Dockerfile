# One image for an app's own worker and the Ziren GPU server it proves against: the
# validated GPU image with the worker's package at the path it is installed at, so the
# paths in the worker's command and settings are the same inside.
#
#   docker build -t NAME:TAG -f deploy/images/gpu-worker/Dockerfile \
#     --build-arg GPU_IMAGE=ghcr.io/projectzkm/ziren-gpu@sha256:DIGEST \
#     --build-arg PACKAGE_PATH=/opt/zkm-fleet/apps/ethproofs \
#     --build-context package=/opt/zkm-fleet/apps/ethproofs deploy/images/gpu-worker
ARG GPU_IMAGE
FROM busybox:1.37.0-musl@sha256:f6a95d611e0a8780fabdb2aec43e31275b2720dfefa3e41b16835bfc42454478 AS busybox
FROM ${GPU_IMAGE}
ARG PACKAGE_PATH
COPY --from=busybox /bin/busybox /usr/local/bin/busybox
COPY --from=package . ${PACKAGE_PATH}/
COPY entrypoint.sh /usr/local/bin/fleet-gpu-worker
ENTRYPOINT ["/usr/local/bin/fleet-gpu-worker"]
