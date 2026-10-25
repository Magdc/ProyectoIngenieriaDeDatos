"""validate_2_3.py: utilidades y flujo con comandos falsos (sin red, Docker ni Terraform)."""

from __future__ import annotations


from ops import services as sv
from ops import validate23 as v23


class FakeRun:
    def __init__(self, responses=None):
        self.calls = []
        self.responses = responses or {}

    def __call__(self, args, *, capture=True, env=None, check=False):
        self.calls.append(list(args))
        key = " ".join(args)
        for pattern, res in self.responses.items():
            if pattern in key:
                return res
        return sv.Result(0, "", "")


def test_pytest_counts():
    out = "....\n=== 89 passed, 2 skipped, 1 failed, 91 deselected, 13 warnings in 12.1s ==="
    assert v23.pytest_counts(out) == {"passed": 89, "failed": 1, "skipped": 2, "errors": 0}
    assert v23.pytest_counts("1 error in 0.1s")["errors"] == 1


def test_plan_del_lote_2_3_solo_admite_el_permiso_de_ar():
    ok = [("google_artifact_registry_repository_iam_member.dataflow_image_reader", "google_artifact_registry_repository_iam_member", ["create"])]
    assert v23.ar_plan_ok(ok) == []
    bad = ok + [("module.x.google_pubsub_topic.t", "google_pubsub_topic", ["delete"])]
    assert v23.ar_plan_ok(bad) == ["module.x.google_pubsub_topic.t (delete)"]


def test_sentinel_from():
    assert v23.sentinel_from({"plan": {"sentinel_event_id": "abc"}}) == "abc"
    assert v23.sentinel_from({}) is None


def test_part_tests_ok_y_falla(monkeypatch):
    good = sv.Result(0, "== 89 passed in 3s ==")
    run = FakeRun({"-k enriched or nlp": good, "src/streaming -q": sv.Result(0, "== 180 passed in 9s =="),
                   "contracts/tests": sv.Result(0, "== 18 passed in 1s ==")})
    v = v23.Validation(run)
    v23.part_tests(v, "python")
    assert [s.status for s in v.steps] == [v23.OK] * 4
    run = FakeRun({"-k enriched or nlp": sv.Result(0, "== 88 passed, 1 skipped in 3s =="),
                   "contracts/tests": sv.Result(0, "== 18 passed ==")})
    v = v23.Validation(run)
    v23.part_tests(v, "python")
    assert v.steps[1].status == v23.FAIL and v.failed


def test_main_sin_codigo_del_lote(monkeypatch, tmp_path):
    monkeypatch.setattr(v23, "PROJECT_DIR", tmp_path)
    monkeypatch.setattr(v23, "EVIDENCE_DIR", tmp_path / "evidencia")
    monkeypatch.setattr(sv, "crlf_scripts", lambda scripts_dir=None: [])
    assert v23.main(["tests", "--yes"], run=FakeRun()) == 2
    assert (tmp_path / "evidencia" / "validacion_2_3.json").exists()


def test_main_solo_tests(monkeypatch, tmp_path):
    (tmp_path / "src" / "streaming").mkdir(parents=True)
    (tmp_path / "src" / "streaming" / "nlp.py").write_text("")
    monkeypatch.setattr(v23, "PROJECT_DIR", tmp_path)
    monkeypatch.setattr(v23, "EVIDENCE_DIR", tmp_path / "evidencia")
    monkeypatch.setattr(sv, "crlf_scripts", lambda scripts_dir=None: [])
    run = FakeRun({"-k enriched or nlp": sv.Result(0, "== 89 passed =="), "src/streaming -q": sv.Result(0, "== 180 passed =="),
                   "contracts/tests": sv.Result(0, "== 18 passed ==")})
    assert v23.main(["tests", "--yes"], run=run) == 0
    assert not any("terraform" in " ".join(c) or "docker" in " ".join(c) for c in run.calls)


def test_sin_argumentos_corre_las_tres_partes(monkeypatch, tmp_path):
    monkeypatch.setattr(v23, "PROJECT_DIR", tmp_path)
    monkeypatch.setattr(v23, "EVIDENCE_DIR", tmp_path / "evidencia")
    monkeypatch.setattr(sv, "crlf_scripts", lambda scripts_dir=None: [])
    seen = {}
    monkeypatch.setattr(sv, "confirm", lambda prompt, s: seen.setdefault("asked", True) and False)
    (tmp_path / "src" / "streaming").mkdir(parents=True)
    (tmp_path / "src" / "streaming" / "nlp.py").write_text("")
    monkeypatch.setenv("GCP_PROJECT", "p")
    assert v23.main([], run=FakeRun()) == 1  # se canceló en la confirmación, tras aceptar los argumentos
    assert seen["asked"]


def test_parte_desconocida():
    import pytest

    with pytest.raises(SystemExit):
        v23.main(["nube"], run=FakeRun())


def test_failed_tests_y_se_detiene_tras_fallar(monkeypatch, tmp_path):
    out = "FAILED src/streaming/tests/test_nlp.py::test_x - AssertionError: boom\n== 1 failed, 88 passed =="
    assert v23.failed_tests(out) == ["src/streaming/tests/test_nlp.py::test_x - AssertionError: boom"]
    (tmp_path / "src" / "streaming").mkdir(parents=True)
    (tmp_path / "src" / "streaming" / "nlp.py").write_text("")
    monkeypatch.setattr(v23, "PROJECT_DIR", tmp_path)
    monkeypatch.setattr(v23, "EVIDENCE_DIR", tmp_path / "evidencia")
    monkeypatch.setattr(sv, "crlf_scripts", lambda scripts_dir=None: [])
    run = FakeRun({"-k enriched or nlp": sv.Result(1, out), "contracts/tests": sv.Result(0, "== 18 passed ==")})
    assert v23.main(["tests", "local", "--yes"], run=run) == 1
    assert not any("docker" in " ".join(c) for c in run.calls)


def test_plan_tolera_update_del_simulador_pero_no_reemplazo():
    sim = "module.simulator[0].google_cloud_run_v2_service.this"
    assert v23.ar_plan_ok([(sim, "google_cloud_run_v2_service", ["update"])]) == []
    assert v23.ar_plan_ok([(sim, "google_cloud_run_v2_service", ["delete", "create"])])


def test_clean_output_quita_ruido_de_compose():
    text = " Container x Running\n✔ y\n[+] up\nResultado (FALLA):\n  [FALLA] dlq_file: esperado 1"
    assert v23.clean_output(text) == "Resultado (FALLA):\n  [FALLA] dlq_file: esperado 1"


def test_e2_distingue_permiso_nuevo_de_deriva_del_simulador():
    sim = ("module.simulator[0].google_cloud_run_v2_service.this", "google_cloud_run_v2_service", ["update"])
    ar = ("google_artifact_registry_repository_iam_member.dataflow_image_reader",
          "google_artifact_registry_repository_iam_member", ["create"])
    assert v23.ar_plan_detail([ar]) == "permiso creado ahora"
    assert v23.ar_plan_detail([sim]) == (
        "el permiso ya existía; además se actualizó en sitio: module.simulator[0].google_cloud_run_v2_service.this (update)")


def test_e4_encuentra_la_imagen_en_cualquier_campo_del_job():
    image = "us-central1-docker.pkg.dev/p/ta-images/dataflow-nlp:20261022224102"
    desc = ('{"environment": {"sdkPipelineOptions": {"options": {"sdk_container_image": "%s"}},'
            ' "workerPools": [{"sdkHarnessContainerImages": [{"containerImage": "%s"}]}]}}' % (image, image))
    assert v23.job_images(desc) == [image]
    assert v23.image_in_job(desc, image)
    assert not v23.image_in_job(desc, "us-central1-docker.pkg.dev/p/ta-images/dataflow-nlp:otra")
    assert v23.job_images('{"sdkContainerImage": null}') == []


def test_e7_espera_una_ventana_completa_de_raw_tras_el_arranque():
    # arranque en t=0; ventanas de 300 s y asentamiento de 600 s: hace falta llegar a 930 s
    assert v23.coverage_wait_s(0, 750) == 180
    assert v23.coverage_wait_s(0, 930) == 0
    assert v23.coverage_wait_s(0, 2000) == 0
