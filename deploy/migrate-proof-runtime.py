#!/usr/bin/env python3
"""Prepare an offline proof-runtime cutover; never stop tasks or edit storage.

The output is private, reviewable configuration for the existing apps.yaml, plus
settings for the existing Fleet app records. It never reads credential contents.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import re

import yaml


def shim_volume(name, volume):
    """A volume in the shim's `path[:options]` form. The shim mounts each volume at its host
    path, so a legacy `host:container[:options]` volume converts only when both are the same."""
    if not isinstance(volume, str):
        raise ValueError(f"{name}: volume {volume!r} must be a string")
    parts = volume.split(':')
    if len(parts) == 1 or len(parts) == 2 and not parts[1].startswith(('/', '{')):
        return volume
    if len(parts) > 3:
        raise ValueError(f"{name}: volume {volume!r} is not path[:options] or host:container[:options]")
    host, container, *options = parts
    if host != container:
        raise ValueError(f"{name}: volume {volume!r} mounts {host} at {container}, but the shim mounts each "
            "volume at its own host path; make the app use the host path, or provide the data at the container path")
    return host + (':' + options[0] if options and options[0] else '')


def convert(config, names, image):
    if not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("use the complete proof-worker image by registry digest")
    result = copy.deepcopy(config)
    selected = {a['name']: a for a in result.get('apps', []) if a.get('name') in names}
    if set(selected) != set(names):
        raise ValueError("every selected entry must exist")
    if len(set(names)) != len(names) or len(selected) != sum(a.get('name') in names for a in result.get('apps', [])):
        raise ValueError("entry names must be unique")
    if not result.get('shim_binary') or not result.get('shim_state_dir') or not result.get('shim_daemon'):
        raise ValueError("configure the persistent shim service before migration")
    updates = []
    for entry in selected.values():
        if entry.get('kind') not in ('ziren-gpu', 'ziren-cpu'):
            raise ValueError("select proof entries")
        if entry.get('disabled') is not True:
            raise ValueError("disable and drain the proof app, then confirm cleanup before preparing its migration")
        root = entry.get('proof_storage_root', '')
        if not Path(root).is_absolute() or not re.fullmatch('[0-9a-fA-F]{64}', entry.get('elf_id', '')):
            raise ValueError("retain an absolute proof_storage_root and the existing full ELF identity")
        cpus = entry.get('cpus', 1)
        if type(cpus) is not int or cpus < 1:
            raise ValueError(f"{entry['name']}: cpus must be a whole number of CPUs, at least 1, as the "
                f"scheduler requires; it is {cpus!r}")
        # The shim fills {name} templates in an entry's environment; its own values stay as written.
        env = {k: str(v).replace('{', '{{').replace('}', '}}') for k, v in (entry.get('env') or {}).items()}
        volumes = [shim_volume(entry['name'], v) for v in entry.get('volumes') or []]
        # Storage paths stay identical: persisted file:// references and completed
        # lease markers remain valid. Only a credential FILE is mounted, read-only.
        def mount(path, template, readonly):
            existing = [v for v in volumes if v.partition(':')[0] in (path, template)]
            if existing:
                options = existing[0].partition(':')[2].split(',')
                if len(existing) > 1 or ('ro' in options) != readonly or readonly and 'rw' in options:
                    raise ValueError('proof storage must be writable and credential mounts read-only, without duplicate destinations')
            else:
                volumes.append(template + (':ro' if readonly else ''))
        mount(root, root, False)
        convert_worker = integrated_worker if entry.get('worker_command') else sdk_worker
        settings = dict(image=image, runtime={'executor': 'shim', 'driver': 'docker'}, env=env, volumes=volumes,
            port_base=0, command=[], **convert_worker(result, entry, env, volumes, mount))
        if entry['kind'] == 'ziren-gpu':
            settings['shm_gb'] = entry.get('shm_gb') or 64
        entry.update(settings)
        # CPUs are a resource, not an update setting: the app record gets them from
        # `fleetctl app scale --cpus`, or the scheduler restores its own count.
        updates.append(dict(entry=entry['name'], app=entry.get('app', entry['name']),
            namespace=entry['namespace'], cpus=cpus, set=settings))
    return result, updates


def sdk_worker(config, entry, env, volumes, mount):
    """Fleet's SDK worker, which takes its proofs from the scheduler."""
    if not config.get('scheduler', '').startswith('https://'):
        raise ValueError("containers need an HTTPS scheduler reachable from the node")
    if not Path(config.get('cluster_token_file', '')).is_absolute():
        raise ValueError("configure an absolute node credential path")
    if config.get('scheduler_ca_file') and not Path(config['scheduler_ca_file']).is_absolute():
        raise ValueError("configure an absolute scheduler CA path")
    env.update(FLEET_NODE_NAME='{name}', FLEET_MACHINE='{machine}',
        FLEET_SCHEDULER='{scheduler}', FLEET_CLUSTER_TOKEN_FILE='{cluster_token_file}',
        FLEET_APP_NAMESPACE='{namespace}', FLEET_APP='{app}', FLEET_APP_ELF_ID='{elf_id}',
        FLEET_APP_STORAGE_ROOT='{proof_storage_root}', FLEET_GPUS='{gpu_count}',
        FLEET_CPUS='{cpu_count}', FLEET_IMAGE='{image}',
        FLEET_BACKEND='cuda' if entry['kind'] == 'ziren-gpu' else entry.get('backend', 'cpu'),
        FLEET_EXIT_WITH_PARENT='false', FLEET_HEALTH_ADDR='0.0.0.0:8080',
        FLEET_GPU_ENDPOINT='http://127.0.0.1:3000/twirp/', ZIREN_SERVER_PORT='3000',
        CUDA_VISIBLE_DEVICES='{gpus}', ZKM_GPU_DEVICES='{gpus}',
        RAYON_NUM_THREADS='{cpu_count}', RUST_LOG=entry.get('rust_log', 'info'))
    mount(config['cluster_token_file'], '{cluster_token_file}', True)
    if config.get('scheduler_ca_file'):
        env['FLEET_SCHEDULER_CA_FILE'] = '{scheduler_ca_file}'
        mount(config['scheduler_ca_file'], '{scheduler_ca_file}', True)
    else:
        env.pop('FLEET_SCHEDULER_CA_FILE', None)
    return dict(container_port=8080, health_path='/healthz', stop_timeout_s=min(entry.get('stop_timeout_s', 30), 90))


def integrated_worker(config, entry, env, volumes, mount):
    """An app's own worker (`worker_command`), which the app's image runs beside its GPU
    server. The entry's environment stays the server's; the image's entrypoint gives the
    worker Fleet's identity and the entry's worker_env_file. The command, working
    directory, environment file and stop grace stay in the entry as they are."""
    if entry['kind'] != 'ziren-gpu' or not 0 < entry.get('worker_stop_timeout_s', 120) <= 3600:
        raise ValueError(f"{entry['name']}: an integrated worker is a ziren-gpu entry that stops within 3600 s")
    env.update(FLEET_NODE_NAME='{name}', FLEET_APP_NAMESPACE='{namespace}', FLEET_APP='{app}',
        FLEET_APP_ELF_ID='{elf_id}', FLEET_APP_STORAGE_ROOT='{proof_storage_root}', FLEET_CPUS='{cpu_count}',
        FLEET_WORKER_RUST_LOG=entry.get('rust_log', 'info'), FLEET_WORKER_READY_TIMEOUT_S='{ready_timeout_s}',
        ZIREN_SERVER_PORT='3000', CUDA_VISIBLE_DEVICES='{gpus}', ZKM_GPU_DEVICES='{gpus}')
    workdir = entry.get('worker_workdir')
    if workdir and not any(under(workdir, v.partition(':')[0]) for v in volumes):
        mount(workdir, workdir, False)
    if entry.get('worker_env_file'):
        env['FLEET_WORKER_ENV_FILE'] = '{worker_env_file}'
        mount(entry['worker_env_file'], '{worker_env_file}', True)
    worker = entry['worker_command'][0]
    hidden = [v.partition(':')[0] for v in volumes if under(worker, v.partition(':')[0])]
    if hidden:
        raise ValueError(f"{entry['name']}: {worker} lies under the volume {hidden[0]}, which would hide the image's "
            "worker; build the image with the package at another path and name that path in worker_command")
    # The GPU server's port, published on the node's loopback as the legacy executor publishes it.
    return dict(container_port=3000)


def under(path, directory):
    return Path(path) == Path(directory) or Path(directory) in Path(path).parents


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apps', type=Path, required=True)
    parser.add_argument('--name', action='append', required=True)
    parser.add_argument('--image', required=True)
    parser.add_argument('--output-dir', type=Path, required=True,
        help='new private review directory, outside the repository; never applied automatically')
    args = parser.parse_args()
    with args.apps.open() as source:
        config, updates = convert(yaml.safe_load(source), args.name, args.image)
    args.output_dir.mkdir(mode=0o700, parents=False, exist_ok=False)
    files = [('apps.yaml', yaml.safe_dump(config, sort_keys=False)),
             ('updates.json', json.dumps(updates, indent=2) + '\n')]
    for update in updates:
        # Names came from the configuration, so reject traversal before using
        # them as review-file names. No files are installed on execution nodes.
        if not re.fullmatch(r'[A-Za-z0-9_.-]{1,63}', update['entry']) or update['entry'] in ('.','..'):
            raise ValueError('invalid entry name')
        files.append((update['entry'] + '.settings.json', json.dumps(update['set'], indent=2) + '\n'))
    for name, content in files:
        fd = os.open(args.output_dir / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())


if __name__ == '__main__':
    main()
