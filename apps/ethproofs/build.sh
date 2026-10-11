#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 2 ]]; then
  echo "usage: $0 EXISTING_GUEST_ELF OUTPUT_DIRECTORY" >&2
  exit 2
fi
fleet_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
guest_elf=$(realpath -- "$1")
mkdir -p -- "$2"
package_dir=$(realpath -- "$2")
ethproofs_source="$fleet_root/third_party/reth-processor"
if [[ ! -f "$guest_elf" || ! -f "$ethproofs_source/bin/eth-proofs/Cargo.toml" ]]; then
  echo "Initialize the reth-processor submodule and supply an existing guest ELF." >&2
  exit 1
fi
if [[ -n $(git -C "$ethproofs_source" status --porcelain) ]]; then
  echo "Commit worker source changes before packaging a pinned release." >&2
  exit 1
fi
export ETH_PROOFS_BUILD_ELF="$guest_elf"
export CARGO_TARGET_DIR="$fleet_root/target/ethproofs-app"
cargo build --locked --release --manifest-path "$ethproofs_source/Cargo.toml" -p eth-proofs
install -m 755 "$CARGO_TARGET_DIR/release/eth-proofs" "$package_dir/eth-proofs"
install -m 644 "$guest_elf" "$package_dir/program.elf"
git -C "$ethproofs_source" rev-parse HEAD > "$package_dir/source-revision"
(cd -- "$package_dir" && sha256sum eth-proofs program.elf > SHA256SUMS)
echo "Packaged EthProofs worker in $package_dir"
