"""Downloads one Hugging Face repo revision into a directory, standard library only.

    python3 stage_model.py REPO REVISION DEST [--include=GLOB]...

Runs inside a fleet run (`run.sh stage-model`). Plain HTTPS with resume: no Xet
or hf_transfer client, which can hang. Every LFS file is checked against the
SHA-256 the Hub publishes; a file already present with the right size and hash
is kept. HF_TOKEN_FILE (if set) authenticates gated repos. On success DEST gets
a STAGED.json manifest; a partial download leaves *.part files and no manifest.
"""

import fnmatch
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request

HUB = "https://huggingface.co"


def _request(url, token, headers=None):
    h = {"User-Agent": "zkm-fleet-stage-model/1"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    h.update(headers or {})
    return urllib.request.Request(url, headers=h)


def list_files(repo, rev, token):
    url = f"{HUB}/api/models/{repo}/tree/{urllib.parse.quote(rev, safe='')}?recursive=true"
    with urllib.request.urlopen(_request(url, token), timeout=60) as r:
        entries = json.load(r)
    return [e for e in entries if e.get("type") == "file"]


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 24), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(repo, rev, entry, dest, token):
    rel = entry["path"]
    final = os.path.join(dest, rel)
    size = entry.get("size", 0)
    want = (entry.get("lfs") or {}).get("oid")
    if os.path.exists(final) and os.path.getsize(final) == size and (not want or sha256(final) == want):
        print(f"have  {rel} ({size} B)", flush=True)
        return
    os.makedirs(os.path.dirname(final), exist_ok=True)
    part = final + ".part"
    url = f"{HUB}/{repo}/resolve/{urllib.parse.quote(rev, safe='')}/{urllib.parse.quote(rel)}"
    for attempt in range(1, 6):
        have = os.path.getsize(part) if os.path.exists(part) else 0
        try:
            req = _request(url, token, {"Range": f"bytes={have}-"} if have else None)
            with urllib.request.urlopen(req, timeout=120) as r, open(part, "ab" if have and r.status == 206 else "wb") as f:
                t0, got = time.time(), 0
                while True:
                    chunk = r.read(1 << 22)
                    if not chunk:
                        break
                    f.write(chunk)
                    got += len(chunk)
                    if time.time() - t0 > 30:
                        print(f"  {rel}: {(have + got) >> 20} / {size >> 20} MiB", flush=True)
                        t0 = time.time()
            break
        except Exception as e:  # network hiccup: resume
            print(f"  {rel}: attempt {attempt} interrupted ({e}); resuming", flush=True)
            time.sleep(10 * attempt)
    else:
        raise SystemExit(f"could not download {rel}")
    if os.path.getsize(part) != size:
        raise SystemExit(f"{rel}: got {os.path.getsize(part)} B, the Hub says {size}")
    if want and sha256(part) != want:
        raise SystemExit(f"{rel}: sha256 differs from the Hub's {want}")
    os.replace(part, final)
    print(f"done  {rel} ({size} B{', sha256 ok' if want else ''})", flush=True)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--include=")]
    include = [a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--include=")]
    repo, rev, dest = args
    token = None
    if os.environ.get("HF_TOKEN_FILE") and os.path.exists(os.environ["HF_TOKEN_FILE"]):
        token = open(os.environ["HF_TOKEN_FILE"]).read().strip()
    files = [e for e in list_files(repo, rev, token)
             if not include or any(fnmatch.fnmatch(e["path"], g) for g in include)]
    total = sum(e.get("size", 0) for e in files)
    print(f"{repo}@{rev}: {len(files)} files, {total / 2**30:.2f} GiB -> {dest}", flush=True)
    for e in files:
        fetch(repo, rev, e, dest, token)
    with open(os.path.join(dest, "STAGED.json"), "w") as f:
        json.dump({"repo": repo, "revision": rev, "include": include, "files": [e["path"] for e in files],
                   "bytes": total, "staged_at": time.time()}, f, indent=2)
    print("STAGED", flush=True)


if __name__ == "__main__":
    main()
