from __future__ import annotations

import logging
from typing import Optional

import httpx


class ElevenLabsClient:
    def __init__(
        self,
        api_key: str | None,
        voice_id: str | None,
        model_id: str = "eleven_multilingual_v2",
        base_url: str = "https://api.elevenlabs.io",
        timeout: float = 30.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.voice_id = (voice_id or "").strip()
        self.model_id = model_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.log = logger or logging.getLogger(__name__)

    def enabled(self) -> bool:
        return bool(self.api_key and self.voice_id)

    def synthesize(self, text: str, duration_seconds: int | None = None) -> bytes:
        if not self.enabled():
            raise RuntimeError("ElevenLabs client is not configured")
        url = f"{self.base_url}/v1/text-to-speech/{self.voice_id}"
        payload = {
            "text": text,
            "model_id": self.model_id,
        }
        if duration_seconds:
            payload["optimize_streaming_latency"] = 1
            payload["voice_settings"] = {"stability": 0.5, "similarity_boost": 0.5}
            payload["timing"] = {
                "type": "wpm_hint",
                "words_per_minute": max(80, int(len(text.split()) / max(duration_seconds / 60, 0.5))),
            }
        headers = {
            "xi-api-key": self.api_key,
            "Content-Type": "application/json",
        }
        with httpx.Client(proxy="http://MKnEA2:hgbt68@168.81.65.13:8000",timeout=self.timeout) as client:
            response = client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            audio = response.content
            self.log.info(
                "elevenlabs synthesis completed",
                extra={
                    "voice_id": self.voice_id,
                    "model_id": self.model_id,
                    "content_length": len(audio),
                },
            )
            return audio
