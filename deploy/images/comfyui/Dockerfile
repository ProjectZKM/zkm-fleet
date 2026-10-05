# Userland for a ComfyUI tree that lives on the host: the tree, its venv and its data
# are mounted at their own paths (services.yaml `volumes`), so the image only has to
# match what the venv was built against: Ubuntu 22.04, /usr/bin/python3.10, and the
# shared libraries its native modules load.
#
# packages.txt = the Ubuntu packages owning every library that `ldd` resolves for the
# .so files in the venv and custom_nodes, on the machine the venv was built on:
#   find venv/lib custom_nodes -name '*.so*' -type f | xargs -n 200 ldd \
#     | awk '/=> \// {print $3}' | sort -u | xargs -n1 readlink -f | xargs -n1 dpkg -S \
#     | cut -d: -f1 | sort -u
# CUDA comes from the driver (the NVIDIA runtime) and torch's own wheels.
#
#   docker build -t zkm-fleet-comfyui:22.04 --build-arg UID=$(id -u) --build-arg GID=$(id -g) deploy/images/comfyui
FROM ubuntu:22.04
ARG UID=1000
ARG GID=1000
ARG USER=ubuntu
ENV DEBIAN_FRONTEND=noninteractive
COPY packages.txt /tmp/packages.txt
# python3.10-dev + build-essential: triton compiles kernels at run time; git: node managers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3.10 python3.10-dev python3.10-venv build-essential \
       ffmpeg git ca-certificates curl $(grep -v '^#' /tmp/packages.txt) \
    && rm -rf /var/lib/apt/lists/* /tmp/packages.txt \
    && groupadd -g "$GID" "$USER" && useradd -m -u "$UID" -g "$GID" -s /bin/bash "$USER"
USER $USER
