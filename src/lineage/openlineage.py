"""Emit OpenLineage run events for every pipeline step.

Each step produces a START event and then COMPLETE or FAIL, with its input and
output datasets. Output datasets carry their schema, row count and, for the
warehouse, column-level lineage; the quality gate reports its checks as data
quality assertions on the tables it checked.

Events are always appended to `data/lineage/events.jsonl`. If OPENLINEAGE_URL
is set (for example http://localhost:5000 for a local Marquez), they are also
POSTed to `<url>/api/v1/lineage`. A lineage backend being down must not stop the
pipeline, so HTTP failures are logged and swallowed; the file copy remains.

Events are built as plain dictionaries following the OpenLineage 2-0-2 spec
rather than through the client library, so the payload is visible in one place.
"""

from __future__ import annotations

import json
import logging
import os
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import requests

log = logging.getLogger("lineage")

PRODUCER = "https://github.com/EST320/governed-data-platform"
SPEC = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
FACET = "https://openlineage.io/spec/facets"
NAMESPACE = "edgar"


def _facet(schema: str, **body) -> dict:
    return {"_producer": PRODUCER, "_schemaURL": f"{FACET}/{schema}", **body}


def dataset(name: str, *, fields: dict[str, str] | None = None,
            column_lineage: dict[str, list[tuple[str, str]]] | None = None,
            row_count: int | None = None,
            assertions: list[dict] | None = None,
            namespace: str = NAMESPACE) -> dict:
    """One input or output dataset with its facets.

    fields          column name -> type
    column_lineage  column name -> [(source dataset, source column), ...]
    row_count       rows written (output statistics)
    assertions      [{"assertion": name, "success": bool, "column": col|None}, ...]
    """
    ds: dict = {"namespace": namespace, "name": name, "facets": {}}
    if fields:
        ds["facets"]["schema"] = _facet(
            "1-1-1/SchemaDatasetFacet.json#/$defs/SchemaDatasetFacet",
            fields=[{"name": k, "type": v} for k, v in fields.items()])
    if column_lineage:
        ds["facets"]["columnLineage"] = _facet(
            "1-2-0/ColumnLineageDatasetFacet.json#/$defs/ColumnLineageDatasetFacet",
            fields={col: {"inputFields": [{"namespace": namespace, "name": src, "field": src_col}
                                          for src, src_col in sources]}
                    for col, sources in column_lineage.items()})
    if row_count is not None:
        ds["outputFacets"] = {"outputStatistics": _facet(
            "1-0-2/OutputStatisticsOutputDatasetFacet.json#/$defs/OutputStatisticsOutputDatasetFacet",
            rowCount=int(row_count))}
    if assertions is not None:
        ds["inputFacets"] = {"dataQualityAssertions": _facet(
            "1-0-1/DataQualityAssertionsDatasetFacet.json#/$defs/DataQualityAssertionsDatasetFacet",
            assertions=assertions)}
    return ds


class Emitter:
    def __init__(self, data_dir: Path, url: str | None = None):
        self.file = data_dir / "lineage" / "events.jsonl"
        self.url = url if url is not None else os.environ.get("OPENLINEAGE_URL")

    def emit(self, event: dict) -> None:
        self.file.parent.mkdir(parents=True, exist_ok=True)
        with self.file.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, default=str) + "\n")
        if self.url:
            try:
                r = requests.post(f"{self.url.rstrip('/')}/api/v1/lineage", json=event, timeout=5)
                r.raise_for_status()
            except requests.RequestException as exc:
                log.warning("could not send lineage event to %s: %s", self.url, exc)


@dataclass
class Run:
    job: str
    inputs: list[dict]
    outputs: list[dict] = field(default_factory=list)
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


def _event(run: Run, state: str, run_facets: dict | None = None) -> dict:
    return {
        "eventType": state,
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": PRODUCER,
        "schemaURL": SPEC,
        "run": {"runId": run.run_id, "facets": run_facets or {}},
        "job": {"namespace": NAMESPACE, "name": run.job, "facets": {}},
        "inputs": run.inputs,
        "outputs": run.outputs,
    }


@contextmanager
def track(emitter: Emitter, job: str, inputs: list[dict],
          outputs: list[dict] | None = None) -> Iterator[Run]:
    """Emit START, run the block, then COMPLETE or FAIL.

    The block can set `run.outputs` once it knows row counts and schemas.
    """
    run = Run(job, inputs, list(outputs or []))
    emitter.emit(_event(run, "START"))
    try:
        yield run
    except Exception as exc:
        emitter.emit(_event(run, "FAIL", {"errorMessage": _facet(
            "1-0-1/ErrorMessageRunFacet.json#/$defs/ErrorMessageRunFacet",
            message=str(exc), programmingLanguage="python",
            stackTrace=traceback.format_exc())}))
        raise
    emitter.emit(_event(run, "COMPLETE"))
