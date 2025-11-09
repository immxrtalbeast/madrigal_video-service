from __future__ import annotations

import json
from typing import Any, Dict

import httpx


class SupabaseStorageClient:
    def __init__(
        self,
        api_url: str | None,
        public_url: str | None,
        bucket: str,
        api_key: str | None,
        timeout: float = 30.0,
    ) -> None:
        self.api_url = (api_url or "").rstrip("/")
        self.public_url_base = (public_url or "").rstrip("/")
        self.bucket = bucket.strip("/")
        self.api_key = (api_key or "").strip()
        self.timeout = timeout
        self._memory: Dict[str, bytes] = {}

    def is_configured(self) -> bool:
        return bool(self.api_url and self.api_key)

    def upload_text(self, path: str, text: str, content_type: str = "text/plain; charset=utf-8") -> str:
        return self.upload_bytes(path, text.encode("utf-8"), content_type)

    def upload_json(self, path: str, payload: dict[str, Any]) -> str:
        body = json.dumps(payload, ensure_ascii=False, indent=2)
        return self.upload_text(path, body, content_type="application/json")

    def upload_bytes(self, path: str, content: bytes, content_type: str = "application/octet-stream") -> str:
        object_path = self._normalize_path(path)
        if not self.is_configured():
            self._memory[object_path] = content
            return self.public_url(object_path)

        url = f"{self.api_url}/storage/v1/object/{self.bucket}/{object_path}"
        headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": content_type,
        }
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(url, headers=headers, content=content)
            if response.status_code not in (200, 201):
                raise ValueError(f"Supabase upload failed: {response.status_code} {response.text}")
        return self.public_url(object_path)

    def public_url(self, path: str) -> str:
        base = self.public_url_base or f"{self.api_url.rstrip('/')}/storage/v1/object/public"
        joined_path = "/".join(part.strip("/") for part in (self.bucket, path))
        return f"{base.rstrip('/')}/{joined_path}"

    def _normalize_path(self, path: str) -> str:
        return path.strip().lstrip("/")
