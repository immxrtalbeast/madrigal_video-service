from __future__ import annotations

import logging
from typing import Optional
import tempfile

import whisper


class LocalWhisperClient:
    def __init__(
        self,
        model_name: str = "base",
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.model_name = model_name
        self.log = logger or logging.getLogger(__name__)
        self._model = None

    def enabled(self) -> bool:
        return True

    def _load_model(self):
        if self._model is None:
            self._model = whisper.load_model(self.model_name)
            self.log.info("local whisper model loaded", extra={"model": self.model_name})

    def transcribe(self, audio: bytes, filename: str = "voiceover.mp3") -> str:
        self._load_model()
        with tempfile.NamedTemporaryFile(suffix=".mp3") as tmp:
            tmp.write(audio)
            tmp.flush()
            result = self._model.transcribe(tmp.name, task="transcribe", verbose=False)
        srt = whisper.utils.format_srt(result["segments"])
        self.log.info(
            "whisper transcription completed (local)",
            extra={"model": self.model_name, "segments": len(result.get("segments", []))},
        )
        return srt
