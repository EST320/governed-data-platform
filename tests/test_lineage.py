import json

import pytest

from src.lineage.openlineage import Emitter, dataset, track


def events(data_dir):
    path = data_dir / "lineage" / "events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_every_step_starts_and_completes(pipeline_dir):
    jobs = {}
    for e in events(pipeline_dir):
        jobs.setdefault(e["job"]["name"], []).append(e["eventType"])
    assert set(jobs) == {"edgar.staging.2024q1", "edgar.staging.2024q2", "edgar.amendments",
                         "edgar.warehouse", "edgar.quality_gate"}
    assert all(states == ["START", "COMPLETE"] for states in jobs.values())


def test_start_and_complete_share_a_run_id(pipeline_dir):
    by_job = {}
    for e in events(pipeline_dir):
        by_job.setdefault(e["job"]["name"], set()).add(e["run"]["runId"])
    assert all(len(ids) == 1 for ids in by_job.values())


def completed(data_dir, job):
    return next(e for e in events(data_dir) if e["job"]["name"] == job and e["eventType"] == "COMPLETE")


def test_warehouse_outputs_carry_column_lineage(pipeline_dir):
    outputs = {o["name"]: o for o in completed(pipeline_dir, "edgar.warehouse")["outputs"]}
    fact = outputs["warehouse.fact_financial_fact"]
    value_sources = fact["facets"]["columnLineage"]["fields"]["value"]["inputFields"]
    assert value_sources == [{"namespace": "edgar", "name": "staging.num", "field": "value"}]
    assert fact["outputFacets"]["outputStatistics"]["rowCount"] == 7
    assert {"name": "value", "type": "DECIMAL(28,4)"} in fact["facets"]["schema"]["fields"]


def test_staging_reports_row_counts_including_quarantine(pipeline_dir):
    outputs = {o["name"]: o for o in completed(pipeline_dir, "edgar.staging.2024q1")["outputs"]}
    assert outputs["staging.num"]["outputFacets"]["outputStatistics"]["rowCount"] == 4
    assert outputs["quarantine.num"]["outputFacets"]["outputStatistics"]["rowCount"] == 0


def test_quality_gate_reports_assertions(pipeline_dir):
    inputs = completed(pipeline_dir, "edgar.quality_gate")["inputs"]
    assertions = {a["assertion"]: a for i in inputs
                  for a in i["inputFacets"]["dataQualityAssertions"]["assertions"]}
    assert assertions["fact_grain_unique"]["success"] is True
    assert assertions["fact_tag_described"]["success"] is False     # the warning is visible too


def test_failure_emits_fail_event_with_error(tmp_path):
    emitter = Emitter(tmp_path, url="")
    with pytest.raises(ValueError):
        with track(emitter, "job.that.fails", inputs=[dataset("x")]):
            raise ValueError("bad input")
    fail = events(tmp_path)[-1]
    assert fail["eventType"] == "FAIL"
    assert fail["run"]["facets"]["errorMessage"]["message"] == "bad input"


def test_unreachable_backend_does_not_break_the_run(tmp_path):
    emitter = Emitter(tmp_path, url="http://127.0.0.1:9")   # nothing listens on port 9
    with track(emitter, "job.ok", inputs=[]):
        pass
    assert [e["eventType"] for e in events(tmp_path)] == ["START", "COMPLETE"]
