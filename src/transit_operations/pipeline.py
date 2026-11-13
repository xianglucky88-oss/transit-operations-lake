from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Callable

from .models import DOMAIN_RECORD_TYPE
from .quality import (
    NORMALIZE_ERROR,
    Outcome,
    Severity,
    UNEXPECTED_ERROR,
    EvaluationContext,
    evaluate,
    truncate_text,
)
from .storage import DataStore


class IngestionPipeline:
    """Coordinates normalization, quality checks and persistence only."""

    def __init__(self, records: Iterable[Any], source: str, store: DataStore, batch_id: int, normalizer: Callable[[Any, str], DOMAIN_RECORD_TYPE]) -> None:
        self.records = records
        self.source = source
        self.store = store
        self.batch_id = batch_id
        self.normalizer = normalizer

    def _persist_rejection(
        self,
        raw: Any,
        source: str | None,
        messages: list[str],
        rule_codes: list[str],
        severity: str | None,
        outcome: str,
        engine_version: str | None,
    ) -> None:
        # Join order follows the fixed rule catalog; the stored text is bounded
        # even when many rules fire on the same record.
        error = truncate_text("; ".join(messages))
        self.store.reject(
            self.batch_id,
            raw,
            source,
            error,
            rule_code=rule_codes[0],
            rule_codes=tuple(rule_codes),
            severity=severity,
            outcome=outcome,
            rule_engine_version=engine_version,
        )

    def run(self) -> dict[str, int]:
        processed = accepted = rejected = changed = warnings = inconclusive = 0
        for raw in self.records:
            processed += 1
            source = raw.get("source") if isinstance(raw, dict) else None
            try:
                record = self.normalizer(raw, self.source)
            except (TypeError, ValueError, KeyError) as exc:
                # Records that fail normalization never reach the rule engine;
                # they are still stored under a stable, reproducible rule code.
                rejected += 1
                self._persist_rejection(
                    raw,
                    source,
                    [str(exc)],
                    [NORMALIZE_ERROR],
                    Severity.ERROR.value,
                    Outcome.REJECT.value,
                    None,
                )
                continue
            try:
                context = EvaluationContext(
                    previous_raw_hash=self.store.previous_raw_hash(record.record_key)
                )
                result = evaluate(record, context=context)
            except Exception as exc:  # engine failure must not abort the batch
                rejected += 1
                self._persist_rejection(
                    raw,
                    source,
                    [f"quality evaluation failed: {type(exc).__name__}: {exc}"],
                    [UNEXPECTED_ERROR],
                    Severity.ERROR.value,
                    Outcome.REJECT.value,
                    None,
                )
                continue

            if result.outcome is Outcome.ACCEPT:
                accepted += 1
                if result.warnings:
                    warnings += 1
                changed += int(self.store.upsert(record))
            else:
                rejected += 1
                if result.outcome is Outcome.INCONCLUSIVE:
                    inconclusive += 1
                blocking = result.blocking
                self._persist_rejection(
                    raw,
                    source,
                    [item.message for item in blocking],
                    [item.rule_code for item in blocking],
                    blocking[0].severity.value if blocking else None,
                    result.outcome.value,
                    result.rule_engine_version,
                )
        self.store.finish_batch(self.batch_id, "completed", processed, accepted, rejected)
        # Batch-scoped counters are returned under their original keys; global
        # table totals are namespaced so they can never overwrite batch numbers
        # (an existing rejected row must not inflate this batch's count).
        result_summary = {
            "processed": processed,
            "accepted": accepted,
            "rejected": rejected,
            "inconclusive": inconclusive,
            "warnings": warnings,
            "changed": changed,
        }
        totals = self.store.counts()
        result_summary.update({f"total_{key}": value for key, value in totals.items()})
        # Preserve the historical flat keys for the tables that were already
        # exposed, for callers depending on the old result shape.
        result_summary["records"] = totals["records"]
        result_summary["batches"] = totals["batches"]
        result_summary["raw_manifests"] = totals["raw_manifests"]
        return result_summary
