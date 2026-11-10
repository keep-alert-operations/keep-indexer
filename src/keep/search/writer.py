from __future__ import annotations
from typing import Callable
from keep.shared.errors import AppError, unavailable

from .projection import Projection, VisibilityProof, build_document, assert_complete, canonical


class ProjectionWriter:
    """Writes immutable authoritative snapshots; returns proof, never moves PG progress."""

    def __init__(self, client, index: str, index_epoch: str, *, max_document_bytes: int):
        self.client, self.index, self.index_epoch = client, index, index_epoch
        self.max_document_bytes = max_document_bytes
        self.written = 0  # Documents the last publish sent; zero when all were visible.

    def publish(
        self,
        items: list[Projection],
        *,
        guard: Callable[[Projection, dict, str], None],
        covered_by_newer: Callable[[Projection, dict], bool],
    ) -> VisibilityProof:
        self.written = 0
        if not items or len(items) > 500:
            raise AppError("projection_batch_limit", "Projection batches contain 1–500 documents")
        latest = {}
        expected = []
        for item in items:
            document = build_document(item)
            guard(item, document, self.index_epoch)  # PG epoch, retirement and immutable work check
            encoded = canonical(document)
            if len(encoded) > self.max_document_bytes:
                raise unavailable("projection_document_capacity")
            key = item.alert_id
            if key in latest:
                previous, old_document = latest[key]
                if previous.tenant_id != item.tenant_id or (
                    previous.version == item.version and document != old_document
                ):
                    raise AppError(
                        "projection_version_conflict", "Equal-version snapshots differ", 409
                    )
                if previous.version > item.version:
                    expected.append((item, document))
                    continue
            latest[key] = (item, document)
            expected.append((item, document))
        try:
            # Check visible equal versions before external_gte, in addition to the
            # mandatory durable immutability guard which prevents concurrent races.
            existing = self._visible(list(latest))
            operations = []
            for item, document in latest.values():
                hit = existing.get(item.alert_id)
                if hit and hit["_version"] == item.version and hit["_source"] != document:
                    raise AppError(
                        "projection_version_conflict",
                        "Existing equal-version document differs",
                        409,
                    )
                # Search-visible identical content already proves coverage. Do
                # not rewrite it and wait for another refresh on every scan.
                if hit and (
                    (hit["_version"] == item.version and hit["_source"] == document)
                    or (hit["_version"] > item.version and covered_by_newer(item, hit["_source"]))
                ):
                    continue
                operations.extend(
                    [
                        {
                            "index": {
                                "_index": self.index,
                                "_id": item.alert_id,
                                "version": item.version,
                                "version_type": "external_gte",
                            }
                        },
                        document,
                    ]
                )
            if (
                len(operations) > 2
                and sum(len(canonical(x)) + 1 for x in operations) > 4 * 1024 * 1024
            ):
                raise AppError("projection_batch_limit", "Projection batch exceeds 4 MiB")
            if operations:
                # Counted before the call: a failed verification may follow a real write.
                self.written = len(operations) // 2
                response = self.client.bulk(operations=operations, refresh="wait_for")
                assert_complete(response)
                if len(response.get("items", [])) != len(operations) // 2:
                    raise unavailable("projection_incomplete")
                for result in response["items"]:
                    item_result = result.get("index", {})
                    assert_complete(item_result)
                    if item_result.get("status") not in {200, 201, 409}:
                        raise unavailable("projection_failed")
            visible = self._visible(list(latest)) if operations else existing
            for item, document in expected:
                hit = visible.get(item.alert_id)
                if not hit or hit["_source"].get("tenant_id") != item.tenant_id:
                    raise unavailable("projection_not_visible")
                version = hit["_version"]
                if version == item.version and hit["_source"] == document:
                    continue
                if version <= item.version or not covered_by_newer(item, hit["_source"]):
                    raise unavailable("projection_coverage_unproven")
            return VisibilityProof(
                self.index_epoch, tuple((i.tenant_id, i.seq, i.alert_id, i.version) for i in items)
            )
        except AppError:
            raise
        except Exception as exc:
            raise unavailable("projection_unavailable") from exc

    def _visible(self, ids: list[str]) -> dict:
        response = self.client.search(
            index=self.index,
            query={"ids": {"values": ids}},
            size=len(ids),
            version=True,
            track_total_hits=True,
            allow_partial_search_results=False,
        )
        assert_complete(response)
        return {hit["_id"]: hit for hit in response["hits"]["hits"]}
