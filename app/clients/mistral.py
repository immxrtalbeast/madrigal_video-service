from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, List, Optional

import httpx


@dataclass
class StoryboardScene:
    title: str
    focus: str
    voiceover: str
    duration_seconds: int


class MistralClient:
    def __init__(
        self,
        api_key: str | None,
        model: str = "mistral-large-latest",
        base_url: str = "https://api.mistral.ai",
        timeout: float = 30.0,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.api_key = (api_key or "").strip()
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.log = logger or logging.getLogger(__name__)

    def enabled(self) -> bool:
        return bool(self.api_key)

    def generate_storyboard(
        self,
        idea: str,
        target_audience: str | None,
        style: str,
        duration_seconds: int,
        num_scenes: int = 5,
        wpm_hint: int = 160,
    ) -> dict[str, Any]:
        if not self.enabled():
            return self._fallback_storyboard(idea, target_audience, style, duration_seconds, num_scenes)

        prompt = self._build_prompt(idea, target_audience, style, duration_seconds, num_scenes, wpm_hint)
        url = f"{self.base_url}/v1/chat/completions"
        payload = {
            "model": self.model,
            "temperature": 0.4,
            "messages": [
                {"role": "system", "content": "You are an assistant that produces JSON storyboards."},
                {"role": "user", "content": prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        with httpx.Client(proxy="http://MKnEA2:hgbt68@168.81.65.13:8000", timeout=self.timeout) as client:
            response = client.post(url, headers=headers, json=payload)
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                self.log.error(
                    "mistral HTTP error",
                    extra={
                        "status": exc.response.status_code if exc.response else None,
                        "body": exc.response.text if exc.response else None,
                    },
                )
                raise
            body = response.json()
            self.log.info("mistral response", extra={"payload": body, "model": self.model})
            try:
                text = self._extract_text(body)
            except ValueError:
                self.log.warning(
                    "mistral response missing structured text, falling back",
                    extra={"payload": body, "model": self.model},
                )
                return self._fallback_storyboard(idea, target_audience, style, duration_seconds, num_scenes)

        storyboard = self._parse_storyboard(text, idea, style)
        storyboard["summary"] = storyboard.get("summary") or f"Вирусный ролик про '{idea}' в стиле {style}."
        return storyboard

    def _extract_text(self, payload: dict[str, Any]) -> str:
        choices = payload.get("choices") or []
        if not choices:
            raise ValueError("Mistral response missing choices")
        message = choices[0].get("message") or {}
        content = message.get("content")
        if not content:
            raise ValueError("Mistral response missing message content")
        return content

    def _parse_storyboard(self, raw: str, idea: str, style: str) -> dict[str, Any]:
        raw = self._strip_code_fence(raw)
        try:
            data = json.loads(raw)
            scenes = data.get("scenes") or []
            parsed_scenes: List[dict[str, Any]] = []
            for idx, scene in enumerate(scenes, start=1):
                parsed_scenes.append(
                    {
                        "position": idx,
                        "title": scene.get("title") or f"Сцена {idx}",
                        "focus": scene.get("focus") or idea,
                        "voiceover": scene.get("voiceover") or "",
                        "visual": scene.get("visual") or style,
                        "duration_seconds": scene.get("duration_seconds") or max(5, 15),
                    }
                )
            data["scenes"] = parsed_scenes
            return data
        except json.JSONDecodeError:
            return self._fallback_storyboard(idea, None, style, 60, 5)

    def _strip_code_fence(self, payload: str) -> str:
        text = payload.strip()
        if text.startswith("```"):
            text = text[3:]
            if text.lower().startswith("json"):
                text = text[4:]
            text = text.lstrip("\n\r")
        if text.endswith("```"):
            text = text[:-3]
        return text.strip()

    def _build_prompt(
        self,
        idea: str,
        target_audience: Optional[str],
        style: str,
        duration_seconds: int,
        num_scenes: int,
        wpm_hint: int,
    ) -> str:
        target = target_audience or "широкая аудитория"
        total_words = max(30, int(duration_seconds * wpm_hint / 60))
        return (
            "Generate JSON storyboard of short vertical video."
            "Struct: {\"summary\": \"...\", \"scenes\": ["
            "{\"title\": str, \"focus\": str, \"voiceover\": str, \"visual\": str, \"duration_seconds\": int}"
            "]}. "
            f"Idea: {idea}. Style: {style}. target_audience: {target}. Duration: {duration_seconds} seconds. "
            f"Voiceover speed is about {wpm_hint} words per minute, so keep the whole script within {total_words} words "
            "and balance each scene accordingly."
            f"Number of scenes: {num_scenes}. Response only valid JSON without comments."
        )

    def _fallback_storyboard(
        self,
        idea: str,
        target_audience: Optional[str],
        style: str,
        duration_seconds: int,
        num_scenes: int,
    ) -> dict[str, Any]:
        per_scene = max(5, duration_seconds // max(1, num_scenes))
        target = target_audience or "широкая аудитория"
        scenes = []
        labels = ["Хук", "Контекст", "Проблема", "Решение", "Призыв"]
        for idx in range(num_scenes):
            label = labels[idx] if idx < len(labels) else f"Сцена {idx + 1}"
            scenes.append(
                {
                    "position": idx + 1,
                    "title": f"{label}: {idea}",
                    "focus": f"{label} для {target}",
                    "voiceover": f"Голос объясняет {label.lower()} идеи '{idea}'.",
                    "visual": f"{style} визуал с акцентом на {label.lower()}",
                    "duration_seconds": per_scene,
                }
            )
        return {
            "summary": f"Вертикальный ролик про '{idea}' для {target}.",
            "scenes": scenes,
        }
