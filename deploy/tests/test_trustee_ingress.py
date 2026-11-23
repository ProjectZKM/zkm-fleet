"""Opt-in real Envoy/Trustee mTLS test. No prover, production state or host ingress."""
import base64
import importlib.util
import json
import os
from pathlib import Path
import ssl
import subprocess
import time
import urllib.error
import urllib.request
import uuid

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(os.environ.get("FLEET_TEST_TRUSTEE_INGRESS") != "1",
                                reason="requires Docker, openssl, protoc, grpcio and TRUSTEE_PROTO")


def run(*args, cwd=None):
    return subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def certificates(root):
    for ca in ("ca", "foreign"):
        run("openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
            "-nodes", "-keyout", f"{ca}.key", "-out", f"{ca}.pem", "-days", "1", "-subj", f"/CN={ca}", cwd=root)
    for name, ca, extensions in (
        ("server", "ca", "subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth"),
        ("node", "ca", "subjectAltName=URI:spiffe://fleet/trustee/node/test-node\nextendedKeyUsage=clientAuth"),
        ("wrong-role", "ca", "subjectAltName=URI:spiffe://fleet/operator/test\nextendedKeyUsage=clientAuth"),
        ("unknown", "foreign", "subjectAltName=URI:spiffe://fleet/trustee/node/test-node\nextendedKeyUsage=clientAuth"),
    ):
        run("openssl", "req", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
            "-keyout", f"{name}.key", "-out", f"{name}.csr", "-subj", f"/CN={name}", cwd=root)
        (root / "extensions").write_text(extensions + "\n")
        run("openssl", "x509", "-req", "-in", f"{name}.csr", "-CA", f"{ca}.pem", "-CAkey", f"{ca}.key",
            "-CAcreateserial", "-out", f"{name}.pem", "-days", "1", "-extfile", "extensions", cwd=root)


@pytest.mark.parametrize("start_as", [True, False])
def test_nodes_can_appraise_but_cannot_change_policy_or_impersonate_identity(tmp_path, start_as):
    import grpc
    proto = Path(os.environ["TRUSTEE_PROTO"]).resolve()
    run("protoc", f"-I{proto.parent}", f"--python_out={tmp_path}", str(proto))
    spec = importlib.util.spec_from_file_location("attestation_test_pb2", tmp_path / "attestation_pb2.py")
    pb = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pb)
    tls = tmp_path / "tls"
    tls.mkdir()
    certificates(tls)
    config = (ROOT / "deploy/gateway.yaml.in").read_text().replace("@SCHEDULER_IP@", "127.0.0.1")
    (tmp_path / "gateway.yaml").write_text(config)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FLEET_", "COMPOSE_"))}
    env["FLEET_SCHEDULER_IMAGE"] = "test:local"
    compose = subprocess.run(["docker", "compose", "--env-file", "/dev/null", "-f",
        str(ROOT / "deploy/docker-compose.yml"), "--profile", "trustee", "config", "--format", "json"], env=env,
        check=True, capture_output=True, text=True)
    services = json.loads(compose.stdout)["services"]
    name = "fleet-trustee-test-" + uuid.uuid4().hex[:12]
    run("docker", "network", "create", name)
    try:
        if start_as:
            run("docker", "run", "-d", "--name", name + "-as", "--network", name, "--network-alias", "trustee-as",
                "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges", "--tmpfs", "/tmp",
                "--tmpfs", "/run/dcap", "--tmpfs", "/opt/confidential-containers/attestation-service",
                "-e", "QCNL_CONF_PATH=/run/dcap/qcnl.conf",
                "--mount", f"type=bind,src={ROOT / 'deploy/trustee/as.json'},dst=/etc/trustee/as.json,readonly", services["trustee-as"]["image"],
                *services["trustee-as"]["command"])
        run("docker", "run", "-d", "--name", name + "-gateway", "--network", name,
            "--user", f"{os.getuid()}:{os.getgid()}", "-p", "127.0.0.1::8905", "--mount", f"type=bind,src={tmp_path},dst=/etc/zkm-fleet,readonly",
            services["gateway"]["image"], "-c", "/etc/zkm-fleet/gateway.yaml", "--log-level", "warn")
        endpoint = run("docker", "port", name + "-gateway", "8905/tcp")
        ca = (tls / "ca.pem").read_bytes()
        def channel(identity):
            return grpc.secure_channel(endpoint, grpc.ssl_channel_credentials(ca,
                (tls / f"{identity}.key").read_bytes() if identity else None,
                (tls / f"{identity}.pem").read_bytes() if identity else None))
        encode = lambda b: base64.urlsafe_b64encode(b).decode().rstrip("=")
        request = pb.AttestationRequest(policy_ids=["default"], verification_requests=[
            pb.IndividualAttestationRequest(tee="sample", evidence=encode(json.dumps({"svn":"1",
                "report_data":base64.b64encode(b"challenge").decode()}).encode()),
                raw_runtime_data=encode(b"challenge"), init_data_digest=encode(bytes([17])*32),
                runtime_data_hash_algorithm="sha256")])
        method = "/attestation.AttestationService/AttestationEvaluate"
        with channel("node") as client:
            grpc.channel_ready_future(client).result(timeout=20)
            rpc = client.unary_unary(method, request_serializer=lambda r: r.SerializeToString(),
                                    response_deserializer=pb.AttestationResponse.FromString)
            if not start_as:
                with pytest.raises(grpc.RpcError) as error:
                    rpc(request, timeout=3)
                assert error.value.code() == grpc.StatusCode.UNAVAILABLE
            else:
                for attempt in range(30):
                    try:
                        assert rpc(request, timeout=2).attestation_token
                        break
                    except grpc.RpcError as error:
                        if error.code() != grpc.StatusCode.UNAVAILABLE or attempt == 29:
                            raise
                        time.sleep(0.2)
            for denied in ("SetAttestationPolicy", "GetAttestationChallenge", "UnknownMethod"):
                with pytest.raises(grpc.RpcError) as error:
                    client.unary_unary("/attestation.AttestationService/" + denied)(b"", timeout=3)
                assert error.value.code() == grpc.StatusCode.PERMISSION_DENIED
        for identity in (None, "wrong-role", "unknown"):
            with channel(identity) as client, pytest.raises(grpc.RpcError) as error:
                client.unary_unary(method)(request.SerializeToString(), timeout=3,
                    metadata=(("x-forwarded-client-cert", "URI=spiffe://fleet/trustee/node/test-node"),))
            assert error.value.code() in (grpc.StatusCode.PERMISSION_DENIED, grpc.StatusCode.UNAVAILABLE)
        # Fleet's original listener still accepts clients without a certificate.
        # This isolated stack has no scheduler, so upstream connection failure is expected.
        context = ssl.create_default_context(cafile=str(tls / "ca.pem"))
        with pytest.raises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(f"https://{endpoint}/healthz", context=context, timeout=5)
        assert error.value.code == 503
    except BaseException:
        for suffix in ("-gateway", "-as"):
            print(subprocess.run(["docker", "logs", name + suffix], capture_output=True, text=True).stderr)
        raise
    finally:
        for suffix in ("-gateway", "-as"):
            subprocess.run(["docker", "rm", "-f", name + suffix], capture_output=True)
        run("docker", "network", "rm", name)
