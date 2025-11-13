from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError


class S3StorageClient:
    def __init__(
        self,
        bucket: str,
        access_key: str | None,
        secret_key: str | None,
        endpoint_url: str | None = None,
        region_name: str | None = None,
        public_url: str | None = None,
        timeout: float = 30.0,
        addressing_style: str | None = None,
    ) -> None:
        self.bucket = (bucket or "").strip()
        self.access_key = (access_key or "").strip()
        self.secret_key = (secret_key or "").strip()
        self.endpoint_url = (endpoint_url or "").rstrip("/") or None
        self.region_name = (region_name or "").strip() or None
        self.public_url_base = (public_url or "").rstrip("/")
        self.timeout = timeout
        self._memory: Dict[str, bytes] = {}
        self._client = None
        if self.is_configured():
            session = boto3.session.Session(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region_name,
            )
            config = BotoConfig(
                s3={"addressing_style": (addressing_style or "virtual").lower()}
            )
            self._client = session.client("s3", endpoint_url=self.endpoint_url, config=config)

    def is_configured(self) -> bool:
        return bool(self.bucket and self.access_key and self.secret_key)

    def upload_text(
        self,
        path: str,
        text: str,
        content_type: str = "text/plain; charset=utf-8",
    ) -> str:
        return self.upload_bytes(path, text.encode("utf-8"), content_type)

    def upload_json(self, path: str, payload: dict[str, Any]) -> str:
        body = json.dumps(payload, ensure_ascii=False, indent=2)
        return self.upload_text(path, body, content_type="application/json")

    def upload_bytes(
        self,
        path: str,
        content: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        key = self._normalize_path(path)
        if not self.is_configured() or self._client is None:
            self._memory[key] = content
            return self.public_url(key)
        try:
            self._client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content,
                ContentType=content_type,
            )
        except (BotoCoreError, ClientError) as exc:  # pragma: no cover - AWS error surface
            raise ValueError(f"S3 upload failed: {exc}") from exc
        return self.public_url(key)

    def list_files(self, prefix: str | None = None) -> List[dict[str, Any]]:
        key_prefix = self._normalize_path(prefix) if prefix else ""
        if not self.is_configured() or self._client is None:
            return self._list_memory(key_prefix)
        contents: List[dict[str, Any]] = []
        kwargs: Dict[str, Any] = {"Bucket": self.bucket}
        if key_prefix:
            kwargs["Prefix"] = key_prefix
        continuation_token: str | None = None
        while True:
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            try:
                response = self._client.list_objects_v2(**kwargs)
            except (BotoCoreError, ClientError) as exc:  # pragma: no cover
                raise ValueError(f"S3 list failed: {exc}") from exc
            for obj in response.get("Contents", []):
                key = obj.get("Key")
                if not key:
                    continue
                contents.append(
                    {
                        "key": key,
                        "size": obj.get("Size"),
                        "last_modified": obj.get("LastModified"),
                        "url": self.public_url(key),
                    }
                )
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")
        return contents

    def download_bytes(self, path: str) -> bytes:
        key = self._normalize_path(path)
        if not self.is_configured() or self._client is None:
            if key not in self._memory:
                raise ValueError("object not found in memory storage")
            return self._memory[key]
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=key)
            body = response.get("Body")
            if body is None:
                return b""
            return body.read()
        except (BotoCoreError, ClientError) as exc:  # pragma: no cover
            raise ValueError(f"S3 download failed: {exc}") from exc

    def public_url(self, path: str) -> str:
        clean = self._normalize_path(path)
        if self.public_url_base:
            return f"{self.public_url_base}/{clean}"
        if self.endpoint_url:
            return f"{self.endpoint_url}/{self.bucket}/{clean}"
        return f"/{self.bucket}/{clean}"

    def _list_memory(self, prefix: str) -> List[dict[str, Any]]:
        items: List[dict[str, Any]] = []
        for key, data in self._memory.items():
            if prefix and not key.startswith(prefix):
                continue
            items.append(
                {
                    "key": key,
                    "size": len(data),
                    "last_modified": datetime.utcnow(),
                    "url": self.public_url(key),
                }
            )
        return items

    def _normalize_path(self, path: str | None) -> str:
        if not path:
            return ""
        return "/".join(part for part in path.strip().split("/") if part)
