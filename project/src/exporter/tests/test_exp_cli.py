import json
from datetime import timedelta

import pytest

from exporter.cli import Deps, main
from exporter.testing import T0, FakeExporterGcs, FakeGcs, FakeRoleS3Client, FakeS3, FakeSts, gcs_key, make_jwt, sa_claims, trust_policy

UID = "112233445566778899001"


class _S3ClientFromFake:
    """Cliente boto3 mínimo encima de ``FakeS3`` para el ``S3Lake`` real."""

    def __init__(self, fake: FakeS3):
        self.fake = fake

    def get_paginator(self, name):
        fake = self.fake

        class P:
            def paginate(self, Bucket, Prefix):  # noqa: N803
                yield {"Contents": [{"Key": k, "Size": s} for k, s in fake.list_prefix(Prefix).items()]}

        return P()

    def put_object(self, **kw):
        r = self.fake.put_object(kw["Key"], kw["Body"], crc32c=kw.get("ChecksumCRC32C"), md5=kw.get("ContentMD5"),
                                 metadata=kw.get("Metadata", {}))
        assert kw["ServerSideEncryption"] == "aws:kms" and kw["BucketKeyEnabled"] is True
        return {"ServerSideEncryption": r["sse"], "SSEKMSKeyId": r["kms_key_id"], "BucketKeyEnabled": True}

    def head_object(self, Bucket, Key, ChecksumMode=None):  # noqa: N803
        h = self.fake.head(Key)
        return {"ContentLength": h["size"], "ServerSideEncryption": h["sse"], "SSEKMSKeyId": h["kms_key_id"],
                "BucketKeyEnabled": True, "ChecksumCRC32C": h["checksum_crc32c"], "Metadata": h["metadata"]}


def _deps(gcs, s3, sts=None, token_claims=None, iam=None):
    claims = token_claims or sa_claims(UID, "sa-exporter@project-0568d9ba-848f-413c-88b.iam.gserviceaccount.com")
    return Deps(
        gcs=lambda cfg: gcs,
        s3_client=lambda session, cfg: s3 if not isinstance(s3, FakeS3) else _S3ClientFromFake(s3),
        token=lambda mode, aud, sa: (make_jwt(claims), "impersonate"),
        sts_client=sts or FakeSts(),
        profile_session=lambda profile, region: object(),
        iam_client=lambda session: iam,
        role_session=lambda assumed, region: object(),
    )


def _gcs():
    gcs = FakeGcs()
    for i in range(2):
        when = T0 + timedelta(minutes=5 * i)
        for source in ("reddit", "facebook", "instagram"):
            gcs.add(gcs_key(source, when), f"{source}{i}".encode() * 7, when + timedelta(minutes=6))
    return gcs


def test_run_y_reporte_json(env, tmp_path, capsys):
    gcs, s3 = _gcs(), FakeS3(kms_key_arn=env["TA_LAKE_KMS_KEY_ARN"])
    out = tmp_path / "run.json"
    code = main(["run", "--json-out", str(out)], env=env, deps=_deps(gcs, s3))
    assert code == 0
    data = json.loads(out.read_text())
    assert data["uploaded"] == 6 and data["aws"]["mode"] == "role"
    assert ":assumed-role/ta-gcp-exporter-role/" in data["aws"]["assumed_arn"]
    logs = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    events = [e["event"] for e in logs]
    assert events.index("export_assumed_role") < events.index("export_start") and events[-1] == "export_done"
    assert not any("SessionToken" in json.dumps(e) or "secreto" in json.dumps(e) for e in logs)


def test_plan_no_sube(env, tmp_path):
    gcs, s3 = _gcs(), FakeS3()
    out = tmp_path / "plan.json"
    assert main(["plan", "--json-out", str(out)], env=env, deps=_deps(gcs, s3)) == 0
    data = json.loads(out.read_text())
    assert data["dry_run"] and data["to_upload"] == 6 and not s3.puts


def test_run_con_falla_sale_1(env):
    gcs, s3 = _gcs(), FakeS3(kms_key_arn=env["TA_LAKE_KMS_KEY_ARN"])
    s3.fail_put.add(sorted(k.replace("enriched/", "raw/origin=stream/") for k in gcs.objects)[0])
    assert main(["run"], env=env, deps=_deps(gcs, s3)) == 1


def test_run_sin_rol_ni_perfil_es_error_de_uso(env, capsys):
    env.pop("TA_EXPORTER_ROLE_ARN")
    assert main(["run"], env=env, deps=_deps(_gcs(), FakeS3())) == 2
    assert "TA_EXPORTER_ROLE_ARN" in capsys.readouterr().err


def test_run_con_asuncion_rechazada_es_error_de_uso(env):
    assert main(["run"], env=env, deps=_deps(_gcs(), FakeS3(), sts=FakeSts(fail="AccessDenied"))) == 2


def test_error_inesperado_queda_en_el_log_y_sale_1(env, capsys):
    class Broken(FakeGcs):
        def read_state(self, key):
            raise RuntimeError("412 Precondition Failed (simulado)")

    assert main(["run"], env=env, deps=_deps(Broken(), FakeS3())) == 1
    logs = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.startswith("{")]
    assert logs[-1]["event"] == "export_error" and "412" in logs[-1]["error"]


def test_state_key_fuera_de_export_state(env):
    assert main(["plan", "--state-key", "enriched/x.json"], env=env, deps=_deps(_gcs(), FakeS3())) == 2


def test_probe_ok_y_resumen(env, tmp_path):
    gcs = FakeExporterGcs()
    gcs.add(gcs_key("reddit", T0), b"pq", T0)
    out = tmp_path / "probe.json"
    assert main(["probe", "--json-out", str(out)], env=env, deps=_deps(gcs, FakeRoleS3Client())) == 0
    data = json.loads(out.read_text())
    assert data["ok"] and data["passed"] == data["total"] == 13


def test_probe_solo_aws(env, tmp_path):
    out = tmp_path / "probe.json"
    assert main(["probe", "--no-gcs", "--json-out", str(out)], env=env, deps=_deps(FakeGcs(), FakeRoleS3Client())) == 0
    assert [r["id"] for r in json.loads(out.read_text())["results"]] == ["A1", "A2", "A3", "A4", "A5", "A6", "A7"]


def test_token_compara_con_la_confianza(env, tmp_path, capsys):
    email = "sa-exporter@project-0568d9ba-848f-413c-88b.iam.gserviceaccount.com"
    trust = tmp_path / "role.json"
    trust.write_text(json.dumps({"Role": {"AssumeRolePolicyDocument": trust_policy(UID, email, "ta-gcp-exporter-role")}}))
    out = tmp_path / "token.json"
    code = main(["token", "--unique-id", UID, "--trust-json", str(trust), "--json-out", str(out)], env=env,
                deps=_deps(FakeGcs(), FakeS3()))
    printed = capsys.readouterr().out
    assert code == 0 and json.loads(out.read_text())["trust_problems"] == []
    assert "eyJ" not in printed  # nunca imprime el token


def test_token_con_otra_cuenta_falla(env, tmp_path):
    trust = tmp_path / "trust.json"
    trust.write_text(json.dumps(trust_policy("999", "otra@x", "ta-gcp-exporter-role")))
    assert main(["token", "--trust-json", str(trust)], env=env, deps=_deps(FakeGcs(), FakeS3())) == 1


def test_token_con_iam_get_role(env):
    class Iam:
        def get_role(self, RoleName):  # noqa: N803
            assert RoleName == "ta-gcp-exporter-role"
            email = "sa-exporter@project-0568d9ba-848f-413c-88b.iam.gserviceaccount.com"
            return {"Role": {"AssumeRolePolicyDocument": trust_policy(UID, email, "ta-gcp-exporter-role")}}

    assert main(["token", "--trust-from-aws"], env=env, deps=_deps(FakeGcs(), FakeS3(), iam=Iam())) == 0


def test_verify_tras_run(env, tmp_path):
    gcs, s3 = _gcs(), FakeS3(kms_key_arn=env["TA_LAKE_KMS_KEY_ARN"])
    deps = _deps(gcs, s3)
    run_json = tmp_path / "run.json"
    assert main(["run", "--json-out", str(run_json)], env=env, deps=deps) == 0
    out = tmp_path / "verify.json"
    assert main(["verify", "--keys-from", str(run_json), "--json-out", str(out)], env=env, deps=deps) == 0
    data = json.loads(out.read_text())
    assert data["status"] == "OK" and next(c for c in data["checks"] if c["id"] == "V4")["sample"] == 6


def test_status_y_doctor_sin_red(env, tmp_path, capsys):
    gcs = _gcs()
    assert main(["status"], env=env, deps=_deps(gcs, FakeS3())) == 0
    assert '"exists": false' in capsys.readouterr().out
    out = tmp_path / "doctor.json"
    code = main(["doctor", "--json-out", str(out)], env=env, deps=_deps(gcs, FakeS3()))
    checks = {c["id"]: c for c in json.loads(out.read_text())["checks"]}
    assert code == 0 and checks["X2"]["status"] == "OK" and checks["X6"]["status"] == "OK"
    assert checks["X7"]["detail"].endswith("no existe todavía")


def test_ayuda_lista_los_subcomandos(capsys):
    with pytest.raises(SystemExit):
        main(["--help"])
    text = capsys.readouterr().out
    for cmd in ("run", "plan", "probe", "token", "doctor", "verify", "status", "lake-report"):
        assert cmd in text
