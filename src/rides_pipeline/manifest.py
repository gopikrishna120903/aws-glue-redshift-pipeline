"""Track which landing files have been processed, using a JSON manifest in S3.

Each run lists the landing prefix, picks only files not yet in the manifest,
and records them once the run succeeds. This makes the pipeline safe to re-run:
a retry after a failure reprocesses the same files, and a normal re-run finds nothing new.
(Glue job bookmarks do the same thing inside Glue; the manifest keeps the logic
visible, testable, and independent of the Glue runtime.)
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class LandingFile:
    key: str
    size: int
    etag: str


class S3Manifest:
    def __init__(self, s3_client, bucket: str, landing_prefix: str, manifest_key: str):
        self.s3 = s3_client
        self.bucket = bucket
        self.landing_prefix = landing_prefix
        self.manifest_key = manifest_key

    def _load(self) -> dict:
        try:
            body = self.s3.get_object(Bucket=self.bucket, Key=self.manifest_key)["Body"].read()
            return json.loads(body)
        except self.s3.exceptions.NoSuchKey:
            return {"processed": {}, "runs": []}

    def list_landing(self) -> list[LandingFile]:
        files = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.landing_prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json") and obj["Size"] > 0:
                    files.append(LandingFile(obj["Key"], obj["Size"], obj["ETag"].strip('"')))
        return sorted(files, key=lambda f: f.key)

    def pending(self) -> list[LandingFile]:
        """Files that are new, or whose content changed (different ETag) since they were processed."""
        processed = self._load()["processed"]
        return [f for f in self.list_landing() if processed.get(f.key) != f.etag]

    def mark_processed(self, files: list[LandingFile], run_id: str, stats: dict | None = None) -> None:
        manifest = self._load()
        for f in files:
            manifest["processed"][f.key] = f.etag
        manifest["runs"].append({
            "run_id": run_id,
            "files": len(files),
            "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "stats": stats or {},
        })
        self.s3.put_object(Bucket=self.bucket, Key=self.manifest_key,
                           Body=json.dumps(manifest, indent=2).encode(), ContentType="application/json")
