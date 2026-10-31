#!/usr/bin/env bash
# Packages the GOAT proof builder worker from the pinned third_party/bitvm-node source.
#
# The package holds proof-builder-rpc, programs.json (guest ELF digests, program IDs,
# verifying-key hashes and the Fleet app id), fleet-app-id, source-revision and
# SHA256SUMS. The build fails unless programs.json equals the supplied baseline.
# --new-baseline instead creates the baseline from this build, with the operator
# verifying-key hash beside it as <baseline>.operator-vk-hash.
set -euo pipefail

usage() {
  echo "usage: $0 BITCOIN_NETWORK BASELINE_JSON OUTPUT_DIRECTORY [--elf-dir DIRECTORY] [--new-baseline]" >&2
  echo "  --elf-dir       reuse validated guest ELFs named <program>.elf instead of building them" >&2
  echo "  --new-baseline  create BASELINE_JSON from this build instead of comparing against it" >&2
  exit 2
}
[[ $# -ge 3 ]] || usage
network=$1
baseline_arg=$2
package_arg=$3
shift 3
elf_dir= new_baseline=
while [[ $# -gt 0 ]]; do
  case "$1" in
    --elf-dir) [[ $# -ge 2 ]] || usage; elf_dir=$2; shift 2 ;;
    --new-baseline) new_baseline=1; shift ;;
    *) usage ;;
  esac
done

case "$network" in
  mainnet | testnet4 | signet | regtest) ;;
  *) echo "BITCOIN_NETWORK must be mainnet, testnet4, signet or regtest, got $network" >&2; exit 2 ;;
esac
if [[ -n $new_baseline ]]; then
  mkdir -p -- "$(dirname -- "$baseline_arg")"
  baseline="$(realpath -- "$(dirname -- "$baseline_arg")")/$(basename -- "$baseline_arg")"
  operator_vk_hash="${baseline%.json}.operator-vk-hash"
  for output in "$baseline" "$operator_vk_hash"; do
    if [[ -e "$output" ]]; then
      echo "$output already exists; remove it to create a new baseline." >&2
      exit 1
    fi
  done
else
  if [[ ! -f "$baseline_arg" ]]; then
    echo "Baseline $baseline_arg does not exist; see apps/goat/README.md." >&2
    exit 1
  fi
  baseline=$(realpath -- "$baseline_arg")
  operator_vk_hash=
fi
if [[ -n "$elf_dir" && ! -d "$elf_dir" ]]; then
  echo "ELF directory $elf_dir does not exist." >&2
  exit 1
fi
mkdir -p -- "$package_arg"
package_dir=$(realpath -- "$package_arg")
elf_dir=${elf_dir:+$(realpath -- "$elf_dir")}
fleet_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
source_dir="$fleet_root/third_party/bitvm-node"

if [[ ! -f "$source_dir/proof-builder-rpc/Cargo.toml" ]]; then
  echo "Initialize the bitvm-node submodule: git submodule update --init third_party/bitvm-node" >&2
  exit 1
fi
if [[ ! -f "$source_dir/circuits/Cargo.toml" ]]; then
  echo "Missing the guest workspace; sync the pinned bitvm-node submodule before building." >&2
  exit 1
fi
if [[ -n $(git -C "$source_dir" status --porcelain --untracked-files=no) ]]; then
  echo "Commit worker source changes before packaging a pinned release." >&2
  exit 1
fi
# The deployed guests are always built with ZKM_IMM_WRAP_VK; the wrap mode is compiled in
# and recorded in the baseline.
export ZKM_IMM_WRAP_VK=1
# The Bitcoin network is compiled into the host and the guests.
export BITCOIN_NETWORK=$network
# Keep the guest workspace's target directory independent of the host build.
# zkm-build also uses CARGO_TARGET_DIR to resolve the ELF paths it embeds.
target_dir="$fleet_root/target/goat-app"
if [[ -f "$HOME/.zkm-toolchain/env" ]]; then
  # shellcheck disable=SC1091
  source "$HOME/.zkm-toolchain/env"
fi
unset CARGO_TARGET_DIR

# Resolve the shared guest workspace target before installing validated ELFs.
elf_layout="$package_dir/guest-elf-paths.json"
python3 - "$source_dir" "$elf_layout" "$elf_dir" <<'PY'
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

source, output, validated_dir = sys.argv[1:]
source = Path(source)

def metadata(manifest, cwd):
    return json.loads(subprocess.check_output([
        "cargo", "metadata", "--locked", "--no-deps", "--format-version", "1",
        "--manifest-path", str(manifest),
    ], cwd=cwd, text=True))

hosts = {package["name"]: package["id"]
         for package in metadata(source / "Cargo.toml", source)["packages"]}
workspace = source / "circuits"
guest = metadata(workspace / "Cargo.toml", workspace)
if Path(guest["workspace_root"]).resolve() != workspace.resolve():
    sys.exit("Expected the shared circuits guest workspace; sync the pinned submodule")
packages = {package["name"]: package for package in guest["packages"]}
layout = {}
for name in ["commit-chain", "header-chain", "operator", "state-chain", "watchtower"]:
    binary = f"{name}-guest"
    if binary not in packages:
        sys.exit(f"Missing {binary}; sync the pinned bitvm-node guest workspace")
    package = packages[binary]
    manifest = workspace / f"{name}-proof/guest/Cargo.toml"
    if Path(package["manifest_path"]).resolve() != manifest.resolve():
        sys.exit(f"Unexpected manifest for {binary}: {package['manifest_path']}")
    binaries = [target["name"] for target in package["targets"]
                if "bin" in target["kind"] and "bin" in target["crate_types"]]
    if binaries != [binary]:
        sys.exit(f"Expected guest binary {binary}, got {binaries}")
    target = Path(guest["target_directory"]) / "elf-compilation/mipsel-zkm-zkvm-elf/release" / binary
    entry = {"host_package_id": hosts[f"{name}-proof"], "env_key": f"ZKM_ELF_{binary}",
             "expected_path": str(target)}
    if validated_dir:
        validated = Path(validated_dir) / f"{name}.elf"
        if not validated.is_file():
            sys.exit(f"Missing validated guest {validated}")
        entry["validated_sha256"] = hashlib.sha256(validated.read_bytes()).hexdigest()
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(validated, target)
        target.chmod(0o644)
    layout[name] = entry
Path(output).write_text(json.dumps(layout, indent=2) + "\n")
PY

if [[ -n "$elf_dir" ]]; then
  export ZKM_SKIP_PROGRAM_BUILD=true
else
  # A source build compiles every guest. An inherited skip flag would embed cached ELFs, and
  # --new-baseline would record them.
  unset ZKM_SKIP_PROGRAM_BUILD
fi

# Rerun the guest-embedding build scripts under this package's environment. A cached run
# keeps the ELF paths and wrap mode of an earlier build, so a switch between --elf-dir and
# source builds could otherwise embed stale guests.
cargo clean --release --target-dir "$target_dir" --manifest-path "$source_dir/Cargo.toml" \
  -p commit-chain-proof -p header-chain-proof -p operator-proof -p state-chain-proof \
  -p watchtower-proof
cargo build --locked --release --target-dir "$target_dir" \
  --manifest-path "$source_dir/Cargo.toml" -p proof-builder-rpc \
  --message-format=json-render-diagnostics > "$package_dir/build-messages.jsonl"
binary="$target_dir/release/proof-builder-rpc"
# The program list becomes programs.json only after it matches (or creates) the baseline.
"$binary" --print-programs > "$package_dir/programs.json.built"

python3 - "$package_dir/programs.json.built" "$elf_layout" "$package_dir/build-messages.jsonl" \
  "$baseline" "$operator_vk_hash" <<'PY'
import difflib
import hashlib
import json
from pathlib import Path
import sys

built_path, layout_path, messages_path, baseline, operator_vk_hash = sys.argv[1:]
built = json.load(open(built_path))
layout = json.load(open(layout_path))
names = ["commit-chain", "header-chain", "operator", "state-chain", "watchtower"]
if [p["name"] for p in built["programs"]] != names:
    sys.exit(f"unexpected programs in the build: {[p['name'] for p in built['programs']]}")
programs = {p["name"]: p for p in built["programs"]}

# Cargo reports the exact build-script environment used for this host build. Do not
# infer embedded paths from directories that may contain stale outputs.
host_names = {entry["host_package_id"]: name for name, entry in layout.items()}
embedded = {}
for line in open(messages_path):
    message = json.loads(line)
    if message.get("reason") != "build-script-executed" or message["package_id"] not in host_names:
        continue
    name = host_names[message["package_id"]]
    path = dict(message["env"]).get(layout[name]["env_key"])
    if not path:
        sys.exit(f"{name} build script did not report {layout[name]['env_key']}")
    if name in embedded and embedded[name] != path:
        sys.exit(f"{name} has conflicting embedded ELF paths")
    embedded[name] = path

digests = {}
for name in names:
    if name not in embedded:
        sys.exit(f"Missing Cargo build-script record for {name}; see {messages_path}")
    path = embedded[name]
    with open(path, "rb") as elf:
        digests[name] = hashlib.sha256(elf.read()).hexdigest()
    if programs[name]["elf_sha256"] != digests[name]:
        sys.exit(f"{name} embeds ELF {programs[name]['elf_sha256']}, not {path}")
    if layout[name].get("validated_sha256", digests[name]) != digests[name]:
        sys.exit(f"{name} did not embed the ELF supplied through --elf-dir")
    layout[name]["embedded_path"] = path
bundle_manifest = "".join(f"{name} {digests[name]}\n" for name in names)
if built["bundle_id"] != hashlib.sha256(bundle_manifest.encode()).hexdigest():
    sys.exit(f"bundle id {built['bundle_id']} does not match the ELF files")

if operator_vk_hash:
    with open(baseline, "x") as output:
        json.dump(built, output, indent=2)
        output.write("\n")
    with open(operator_vk_hash, "x") as output:
        output.write(programs["operator"]["vk_hash"] + "\n")
    print(f"Wrote {baseline} and {operator_vk_hash}")
elif json.load(open(baseline)) != built:
    expected = json.dumps(json.load(open(baseline)), indent=2, sort_keys=True).splitlines()
    actual = json.dumps(built, indent=2, sort_keys=True).splitlines()
    sys.stderr.write("programs.json differs from the baseline:\n")
    sys.stderr.write("\n".join(difflib.unified_diff(expected, actual, "baseline", "built", lineterm="")) + "\n")
    sys.exit(1)
Path(layout_path).write_text(json.dumps(layout, indent=2) + "\n")
PY
mv -- "$package_dir/programs.json.built" "$package_dir/programs.json"

python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["bundle_id"])' \
  "$package_dir/programs.json" > "$package_dir/fleet-app-id"
install -m 755 "$binary" "$package_dir/proof-builder-rpc"
git -C "$source_dir" rev-parse HEAD > "$package_dir/source-revision"
if command -v sha256sum > /dev/null; then
  (cd -- "$package_dir" && sha256sum proof-builder-rpc programs.json fleet-app-id > SHA256SUMS)
else
  (cd -- "$package_dir" && shasum -a 256 proof-builder-rpc programs.json fleet-app-id > SHA256SUMS)
fi
echo "Packaged GOAT proof builder goat/$(cat "$package_dir/fleet-app-id") in $package_dir"
