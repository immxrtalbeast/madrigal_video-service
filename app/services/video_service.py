from __future__ import annotations

import os
import pathlib
import tempfile
import time
from datetime import datetime
import logging
import subprocess
from typing import Any, List
from uuid import UUID, uuid4

import httpx
from PIL import Image
from moviepy.editor import AudioFileClip, ColorClip, ImageClip, CompositeAudioClip, concatenate_videoclips

if not hasattr(Image, "ANTIALIAS"):
    Image.ANTIALIAS = Image.LANCZOS

import re

from app.clients.gemini import GeminiClient, GeminiServiceUnavailable
from app.clients.mistral import MistralClient
from app.clients.s3_storage import S3StorageClient
from app.clients.tts import ElevenLabsClient
from app.clients.whisper import LocalWhisperClient
from app.config import Settings
from app.models.api import (
    DraftApprovalRequest,
    IdeaExpansionRequest,
    MediaAsset,
    SubtitlesApprovalRequest,
    SubtitleStyleRequest,
    VideoGenerationRequest,
)
from app.models.domain import (
    VideoJob,
    VideoJobArtifact,
    VideoJobStage,
    VideoJobStatusHistory,
)
from app.events.publisher import JobEventPublisher
from app.queue.queue import BaseQueue
from app.storage.repository import VideoJobRepository

BACKGROUND_LIBRARY = {
    "test": [
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/07d47b1443cf8ec_big.jpg",
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/d3b67dbf5b0f4547a7d9d1704f1e5ecf.jpg",
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/dadwa288da21dap.png",
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/dwadzxczf1ff31fasf.png",
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/xczsidaid124.png",
    ],
}


class VideoService:
    def __init__(self, repo: VideoJobRepository, settings: Settings) -> None:
        self.repo = repo
        self.queue = None
        self.settings = settings
        self.log = logging.getLogger(__name__)
        self.gemini = GeminiClient(
            api_key=settings.gemini_api_key,
            model=settings.gemini_model,
            logger=self.log,
        )
        self.mistral = None
        if settings.mistral_api_key:
            self.mistral = MistralClient(
                api_key=settings.mistral_api_key,
                model=settings.mistral_model,
                base_url=settings.mistral_base_url,
                logger=self.log,
            )
        self.tts = None
        if settings.tts_provider.lower() == "elevenlabs":
            self.tts = ElevenLabsClient(
                api_key=settings.elevenlabs_api_key,
                voice_id=settings.elevenlabs_voice_id,
                model_id=settings.elevenlabs_model_id,
                base_url=settings.elevenlabs_base_url,
                logger=self.log,
            )
        self.whisper = None
        if settings.whisper_local_model:
            self.whisper = LocalWhisperClient(
                model_name=settings.whisper_local_model,
                logger=self.log,
            )
        self.storage = S3StorageClient(
            bucket=settings.s3_bucket,
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            public_url=settings.s3_public_url,
            addressing_style=settings.s3_addressing_style,
        )
        self.events: JobEventPublisher | None = None
        if settings.kafka_enabled and settings.kafka_updates_topic:
            try:
                self.events = JobEventPublisher(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    topic=settings.kafka_updates_topic,
                    logger=self.log,
                )
            except Exception:  # pragma: no cover - best effort logging
                self.log.warning(
                    "job event publisher unavailable",
                    extra={"topic": settings.kafka_updates_topic},
                    exc_info=True,
                )
        self.voice_catalog: list[dict[str, str]] = settings.voice_catalog or []
        self.music_catalog: list[dict[str, str]] = settings.music_catalog or []

    def bind_queue(self, queue: BaseQueue) -> None:
        self.queue = queue

    def create_job(self, payload: VideoGenerationRequest) -> VideoJob:
        assets_folder = self._build_assets_folder()
        job = VideoJob(
            id=uuid4(),
            idea=payload.idea.strip(),
            language=payload.language or self.settings.default_language,
            duration_seconds=payload.duration_seconds or 45,
            style=payload.style or self.settings.default_style,
            target_audience=payload.target_audience,
            template_id=payload.template_id,
            status=VideoJobStage.QUEUED.value,
            stage=VideoJobStage.QUEUED,
            status_history=[
                VideoJobStatusHistory(
                    status=VideoJobStage.QUEUED.value,
                    stage=VideoJobStage.QUEUED,
                    message="Job enqueued",
                )
            ],
            assets_folder=assets_folder,
            artifacts=[],
            storyboard_summary=None,
            voice_profile=payload.voice_profile or payload.voice_id or self.settings.tts_voice,
            voice_id=payload.voice_id or payload.voice_profile or self.settings.elevenlabs_voice_id,
            soundtrack=payload.soundtrack or self.settings.backing_track,
            soundtrack_url=payload.soundtrack_url,
            subtitles_url=None,
            subtitles_text=None,
            subtitle_batch_size=None,
            subtitle_style=None,
            video_url=None,
            error=None,
        )
        self.repo.save(job)
        if self.queue is not None:
            self.queue.enqueue(job.id)
        self._emit_job_update(job)
        return job

    def get_job(self, job_id: UUID) -> VideoJob:
        job = self.repo.get(job_id)
        if not job:
            raise ValueError("Video job not found")
        return job

    def list_jobs(self) -> list[VideoJob]:
        return self.repo.list()

    def process_job(self, job_id: UUID) -> None:
        job = self.repo.get(job_id)
        if not job:
            return
        try:
            self._pipeline(job)
        except Exception as exc:  # pragma: no cover - defensive
            self.log.exception("video job failed", extra={"job_id": str(job_id)})
            self._update_status(job, VideoJobStage.FAILED, "Video generation failed", error=str(exc))

    def expand_idea(self, payload: IdeaExpansionRequest) -> dict[str, object]:
        tone = payload.tone or self.settings.default_tone
        style = payload.output_format or "vertical short"
        summary = (
            f"{payload.idea.strip().capitalize()} — эмоциональный ролик в тоне {tone}, "
            f"ориентированный на {payload.target_audience or 'широкую аудиторию'}, "
            f"формат {style}, длительность {payload.duration_seconds or 45} секунд."
        )
        return {"summary": summary}

    def _pipeline(self, job: VideoJob) -> None:
        try:
            if job.stage in (VideoJobStage.QUEUED, VideoJobStage.DRAFTING):
                storyboard = self._generate_storyboard(job)
                self._await_draft_review(job, storyboard)
                return
            if job.stage == VideoJobStage.DRAFT_REVIEW:
                storyboard = {
                    "summary": job.storyboard_summary or job.idea,
                    "scenes": job.storyboard or [],
                }
                if not storyboard["scenes"]:
                    raise ValueError("missing storyboard scenes for continuation")
                self._run_post_draft(job, storyboard)
                return
            if job.stage == VideoJobStage.SUBTITLE_REVIEW:
                storyboard = {
                    "summary": job.storyboard_summary or job.idea,
                    "scenes": job.storyboard or [],
                }
                if not storyboard["scenes"]:
                    raise ValueError("missing storyboard scenes for continuation")
                self._finalize_video(job, storyboard)
                return
            self.log.debug(
                "pipeline invocation skipped",
                extra={"job_id": str(job.id), "stage": job.stage.value},
            )
        except Exception as exc:  # pragma: no cover
            self._update_status(job, VideoJobStage.FAILED, "Video generation failed", error=str(exc))

    def _generate_storyboard(self, job: VideoJob) -> dict[str, Any]:
        self.log.debug("starting storyboard drafting", extra={"job_id": str(job.id)})
        storyboard: dict[str, Any] | None = None
        try:
            storyboard = self.gemini.generate_storyboard(
                idea=job.idea,
                target_audience=job.target_audience,
                style=job.style or self.settings.default_style,
                duration_seconds=job.duration_seconds,
                wpm_hint=130,
            )
        except GeminiServiceUnavailable:
            self.log.warning(
                "gemini unavailable, considering mistral fallback",
                extra={"job_id": str(job.id)},
            )
            if self.mistral and self.mistral.enabled():
                storyboard = self.mistral.generate_storyboard(
                    idea=job.idea,
                    target_audience=job.target_audience,
                    style=job.style or self.settings.default_style,
                    duration_seconds=job.duration_seconds,
                    wpm_hint=130,
                )
            else:
                raise
        self._update_status(job, VideoJobStage.DRAFTING, "Drafted storyboard via Gemini")
        job.storyboard_summary = storyboard.get("summary")
        storyboard_path = f"{job.assets_folder}/storyboard.json"
        storyboard_url = self.storage.upload_json(storyboard_path, storyboard)
        self._add_artifact(
            job,
            kind="storyboard",
            path=storyboard_path,
            url=storyboard_url,
            metadata={"model": self.settings.gemini_model},
        )
        time.sleep(0.05)
        return storyboard

    def _await_draft_review(self, job: VideoJob, storyboard: dict[str, Any]) -> None:
        job.storyboard = storyboard.get("scenes", [])
        job.storyboard_summary = storyboard.get("summary")
        self._update_status(job, VideoJobStage.DRAFT_REVIEW, "Awaiting storyboard approval")

    def approve_draft(self, job_id: UUID, payload: DraftApprovalRequest) -> VideoJob:
        job = self.get_job(job_id)
        if job.stage != VideoJobStage.DRAFT_REVIEW:
            raise ValueError("draft stage is not awaiting approval")
        storyboard = self._merge_storyboard(job, payload)
        job.storyboard = storyboard.get("scenes", [])
        job.storyboard_summary = storyboard.get("summary")
        storyboard_path = f"{job.assets_folder}/storyboard.json"
        self.storage.upload_json(storyboard_path, storyboard)
        self._update_status(job, VideoJobStage.DRAFT_REVIEW, "Storyboard approved. Queued for asset build-out")
        if self.queue is not None:
            self.queue.enqueue(job.id)
        else:  # pragma: no cover - fallback for misconfiguration
            self._run_post_draft(job, storyboard)
        return job

    def _run_post_draft(self, job: VideoJob, storyboard: dict[str, Any]) -> None:
        selected_backgrounds = self._select_backgrounds(job, storyboard)
        self._update_status(job, VideoJobStage.ASSETS, "Preparing visual asset prompts")
        frames_payload = {
            "provider": self.settings.text2img_provider,
            "style": job.style,
            "scenes": storyboard.get("scenes", []),
        }
        if selected_backgrounds:
            frames_payload["backgrounds"] = selected_backgrounds
            for idx, scene in enumerate(frames_payload["scenes"]):
                scene["background_url"] = scene.get("background_url") or selected_backgrounds[idx % len(selected_backgrounds)]
        frames_path = f"{job.assets_folder}/frames.json"
        frames_url = self.storage.upload_json(frames_path, frames_payload)
        self._add_artifact(
            job,
            kind="frames",
            path=frames_path,
            url=frames_url,
            metadata={"aspect_ratio": "9:16"},
        )
        time.sleep(0.05)

        self._update_status(job, VideoJobStage.AUDIO, "Creating voiceover script and music plan")
        voice_script = "\n".join(
            scene.get("voiceover", "") for scene in storyboard.get("scenes", []) if scene.get("voiceover")
        ) or f"Озвучка для ролика '{job.idea}'."
        voice_script_path = f"{job.assets_folder}/audio/voiceover.txt"
        voice_script_url = self.storage.upload_text(
            voice_script_path,
            voice_script,
            content_type="text/plain; charset=utf-8",
        )
        self._add_artifact(
            job,
            kind="voiceover_script",
            path=voice_script_path,
            url=voice_script_url,
            metadata={"voice": job.voice_profile},
        )

        voice_audio_url: str | None = None
        voice_audio_path: str | None = None
        audio_bytes: bytes | None = None
        if self.tts and self.tts.enabled():
            audio_bytes = self.tts.synthesize(voice_script, job.duration_seconds, voice_id=job.voice_id)
            voice_audio_path = f"{job.assets_folder}/audio/voiceover.mp3"
            voice_audio_url = self.storage.upload_bytes(
                voice_audio_path,
                audio_bytes,
                content_type="audio/mpeg",
            )
            self._add_artifact(
                job,
                kind="voiceover",
                path=voice_audio_path,
                url=voice_audio_url,
                metadata={
                    "tts_provider": self.settings.tts_provider,
                    "voice": job.voice_profile,
                    "model": self.settings.elevenlabs_model_id,
                },
            )
        else:
            self._add_artifact(
                job,
                kind="voiceover",
                path=voice_script_path,
                url=voice_script_url,
                metadata={"provider": "text-only"},
            )

        resolved_soundtrack_url, soundtrack_meta = self._resolve_soundtrack_source(job)
        soundtrack_plan = [
            f"Soundtrack preset: {job.soundtrack or self.settings.backing_track}",
        ]
        if job.soundtrack_url:
            soundtrack_plan.append(f"Requested URL: {job.soundtrack_url}")
        if resolved_soundtrack_url:
            soundtrack_plan.append(f"Resolved URL: {resolved_soundtrack_url}")
        if not resolved_soundtrack_url:
            soundtrack_plan.append("Fallback: no downloadable soundtrack configured.")
        soundtrack_path = f"{job.assets_folder}/audio/soundtrack.txt"
        soundtrack_url = self.storage.upload_text(
            soundtrack_path,
            "\n".join(soundtrack_plan),
        )
        soundtrack_metadata: dict[str, Any] = {
            "preset": job.soundtrack,
        }
        if job.soundtrack_url:
            soundtrack_metadata["requested_url"] = job.soundtrack_url
        if resolved_soundtrack_url:
            soundtrack_metadata["resolved_url"] = resolved_soundtrack_url
        if soundtrack_meta:
            soundtrack_metadata.update({k: v for k, v in soundtrack_meta.items() if v})
        self._add_artifact(
            job,
            kind="soundtrack",
            path=soundtrack_path,
            url=soundtrack_url,
            metadata=soundtrack_metadata,
        )

        subtitles_text = self._build_subtitles(
            storyboard.get("scenes", []),
            job.subtitle_batch_size,
            job.subtitle_style,
        )
        if audio_bytes and self.whisper and self.whisper.enabled() and not job.subtitle_batch_size:
            try:
                subtitles_text = self.whisper.transcribe(audio_bytes)
            except Exception as exc:  # pragma: no cover
                self.log.warning(
                    "whisper transcription failed",
                    extra={"job_id": str(job.id)},
                    exc_info=exc,
                )
        subtitles_path = f"{job.assets_folder}/subtitles.srt"
        subtitles_url = self.storage.upload_text(
            subtitles_path,
            subtitles_text,
            content_type="application/x-subrip; charset=utf-8",
        )
        job.subtitles_url = subtitles_url
        job.subtitles_text = subtitles_text
        self._add_artifact(
            job,
            kind="subtitles",
            path=subtitles_path,
            url=subtitles_url,
            metadata={"source": "whisper"},
        )
        time.sleep(0.05)

        self._update_status(job, VideoJobStage.SUBTITLE_REVIEW, "Awaiting subtitle approval")

    def approve_subtitles(self, job_id: UUID, payload: SubtitlesApprovalRequest) -> VideoJob:
        job = self.get_job(job_id)
        if job.stage != VideoJobStage.SUBTITLE_REVIEW:
            raise ValueError("subtitles are not awaiting approval")
        text = (payload.text or "").strip()
        if payload.words_per_batch:
            job.subtitle_batch_size = payload.words_per_batch
            if text:
                text = self._rebatch_subtitles(text, payload.words_per_batch, payload.style)
            else:
                if not job.storyboard:
                    raise ValueError("no storyboard available to regenerate subtitles")
                text = self._build_subtitles(job.storyboard, job.subtitle_batch_size, payload.style)
        elif not text:
            if not job.storyboard:
                raise ValueError("no storyboard available to regenerate subtitles")
            text = self._build_subtitles(job.storyboard, job.subtitle_batch_size, payload.style)
        if payload.style:
            job.subtitle_style = payload.style.model_dump(exclude_none=True)
        elif job.subtitle_style is None and payload.words_per_batch:
            job.subtitle_style = {}
        if not text:
            raise ValueError("subtitles text cannot be empty")
        subtitles_path = f"{job.assets_folder}/subtitles.srt"
        subtitles_url = self.storage.upload_text(
            subtitles_path,
            text,
            content_type="application/x-subrip; charset=utf-8",
        )
        job.subtitles_text = text
        job.subtitles_url = subtitles_url
        artifact = self._find_artifact(job, "subtitles")
        if artifact:
            artifact.path = subtitles_path
            artifact.url = subtitles_url
        else:
            self._add_artifact(job, kind="subtitles", path=subtitles_path, url=subtitles_url)
        self.repo.save(job)
        self._update_status(job, VideoJobStage.SUBTITLE_REVIEW, "Subtitles approved. Queued for rendering")
        if self.queue is not None:
            self.queue.enqueue(job.id)
        else:  # pragma: no cover
            storyboard = {
                "summary": job.storyboard_summary or job.idea,
                "scenes": job.storyboard or [],
            }
            self._finalize_video(job, storyboard)
        return job

    def _finalize_video(self, job: VideoJob, storyboard: dict[str, Any]) -> None:
        self._update_status(job, VideoJobStage.RENDERING, "Building final reel manifest")
        manifest = {
            "idea": job.idea,
            "style": job.style,
            "duration_seconds": job.duration_seconds,
            "assets": [artifact.model_dump() for artifact in job.artifacts],
        }
        manifest_path = f"{job.assets_folder}/render_manifest.json"
        manifest_url = self.storage.upload_json(manifest_path, manifest)
        self._add_artifact(job, kind="manifest", path=manifest_path, url=manifest_url)

        selected_backgrounds = self._select_backgrounds(job, storyboard)
        audio_bytes = self._load_voiceover_audio(job)
        subtitles_text = self._load_subtitles_text(job)
        video_path = f"{job.assets_folder}/final.mp4"
        audio_duration = self._get_audio_duration(audio_bytes)
        video_bytes = self._render_video(
            job,
            storyboard,
            selected_backgrounds or [],
            audio_bytes,
            subtitles_text if subtitles_text.strip() else None,
            audio_duration=audio_duration,
        )
        if video_bytes:
            video_url = self.storage.upload_bytes(video_path, video_bytes, content_type="video/mp4")
        else:
            video_url = self.storage.upload_bytes(video_path, b"", content_type="video/mp4")
        job.video_url = video_url
        self._add_artifact(
            job,
            kind="video",
            path=video_path,
            url=video_url,
            metadata={"status": "placeholder"},
        )
        time.sleep(0.05)

        self._update_status(job, VideoJobStage.READY, "Video is ready for download", video_url=video_url)

    def _update_status(
        self,
        job: VideoJob,
        stage: VideoJobStage,
        message: str,
        video_url: str | None = None,
        error: str | None = None,
    ) -> None:
        job.status = stage.value
        job.stage = stage
        job.status_history.append(
            VideoJobStatusHistory(
                status=stage.value,
                stage=stage,
                message=message,
            )
        )
        job.updated_at = datetime.utcnow()
        if video_url:
            job.video_url = video_url
        if error:
            job.error = error
        self.repo.save(job)
        self._emit_job_update(job)

    def upload_media(
        self,
        folder: str,
        filename: str | None,
        data: bytes,
        *,
        user_id: str,
        content_type: str | None = None,
    ) -> MediaAsset:
        if not folder or not folder.strip():
            raise ValueError("folder is required")
        safe_name = pathlib.PurePosixPath(filename or "").name
        if not safe_name:
            safe_name = f"asset-{uuid4().hex}"
        prefix = self._user_media_prefix(folder, user_id)
        key = "/".join(part for part in (prefix, safe_name) if part)
        url = self.storage.upload_bytes(
            key,
            data,
            content_type=content_type or "application/octet-stream",
        )
        return MediaAsset(key=key, url=url, size=len(data))

    def list_media(self, folder: str | None, user_id: str) -> list[MediaAsset]:
        prefix = self._user_media_prefix(folder or "", user_id)
        try:
            objects = self.storage.list_files(prefix)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        assets: list[MediaAsset] = []
        for obj in objects:
            key = obj.get("key")
            if not key:
                continue
            if key.rstrip().endswith("/"):
                continue
            assets.append(
                MediaAsset(
                    key=key,
                    url=obj.get("url", ""),
                    size=obj.get("size"),
                    last_modified=obj.get("last_modified"),
                )
            )
        return assets

    def list_shared_media(self, folder: str | None = None) -> list[MediaAsset]:
        prefix = self._compose_shared_prefix(folder)
        try:
            objects = self.storage.list_files(prefix)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        assets: list[MediaAsset] = []
        for obj in objects:
            key = obj.get("key")
            if not key or not self._is_image_asset(key):
                continue
            assets.append(
                MediaAsset(
                    key=key,
                    url=obj.get("url", ""),
                    size=obj.get("size"),
                    last_modified=obj.get("last_modified"),
                )
            )
        return assets

    def list_voices(self) -> list[dict[str, str]]:
        if self.voice_catalog:
            normalized: list[dict[str, str]] = []
            for entry in self.voice_catalog:
                voice_id = entry.get("voice_id") or entry.get("id")
                if not voice_id:
                    continue
                normalized.append(
                    {
                        "voice_id": voice_id,
                        "name": entry.get("name"),
                        "description": entry.get("description"),
                        "preview_url": entry.get("preview_url") or entry.get("url"),
                    }
                )
            return normalized
        if self.settings.elevenlabs_voice_id:
            return [
                {
                    "voice_id": self.settings.elevenlabs_voice_id,
                    "name": "Default voice",
                    "description": "Configured voice from settings",
                }
            ]
        return []

    def list_music(self) -> list[dict[str, str]]:
        if not self.music_catalog:
            return []
        items: list[dict[str, str]] = []
        for entry in self.music_catalog:
            name = (entry.get("name") or "").strip()
            url = (entry.get("url") or "").strip()
            if not name or not url:
                continue
            items.append(
                {
                    "name": name,
                    "description": entry.get("description"),
                    "author": entry.get("author"),
                    "url": url,
                    "low_volume": entry.get("low_volume"),
                }
            )
        return items

    def _build_assets_folder(self) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        prefix = self.settings.storage_folder_prefix.strip("/")
        parts = [prefix, f"{timestamp}-{uuid4().hex[:8]}"] if prefix else [f"{timestamp}-{uuid4().hex[:8]}"]
        return "/".join(parts)

    def _compose_media_prefix(self, folder: str | None) -> str:
        folder_segment = self._normalize_storage_segment(folder)
        root_segment = self._normalize_storage_segment(self.settings.media_root_prefix)
        if folder_segment and root_segment and folder_segment.startswith(root_segment):
            return folder_segment
        parts = [segment for segment in (root_segment, folder_segment) if segment]
        return "/".join(parts)

    def _user_media_prefix(self, folder: str, user_id: str) -> str:
        folder_segment = self._normalize_storage_segment(folder)
        user_segment = self._normalize_storage_segment(user_id)
        root_segment = self._normalize_storage_segment(self.settings.media_root_prefix)
        parts = [segment for segment in (root_segment, user_segment, folder_segment) if segment]
        return "/".join(parts)

    def _compose_shared_prefix(self, folder: str | None) -> str:
        shared_root = self._normalize_storage_segment(self.settings.shared_media_prefix)
        folder_segment = self._normalize_storage_segment(folder)
        parts = [segment for segment in (shared_root, folder_segment) if segment]
        return "/".join(parts)

    def _normalize_storage_segment(self, value: str | None) -> str:
        if not value:
            return ""
        cleaned: list[str] = []
        for part in value.strip().split("/"):
            part = part.strip()
            if not part or part in (".", ".."):
                continue
            cleaned.append(part)
        return "/".join(cleaned)

    def _is_image_asset(self, key: str) -> bool:
        if not key or key.rstrip().endswith("/"):
            return False
        lowered = key.lower()
        return lowered.endswith((".png", ".jpg", ".jpeg", ".webp"))

    def _add_artifact(
        self,
        job: VideoJob,
        kind: str,
        path: str,
        url: str,
        metadata: dict[str, object] | None = None,
    ) -> VideoJobArtifact:
        artifact = VideoJobArtifact(
            kind=kind,
            path=path,
            url=url,
            metadata=metadata or {},
        )
        job.artifacts.append(artifact)
        self.repo.save(job)
        self._emit_job_update(job)
        return artifact

    def _emit_job_update(self, job: VideoJob) -> None:
        if not self.events:
            return
        try:
            self.events.publish_job(job)
        except Exception:  # pragma: no cover
            self.log.warning("job event emission failed", extra={"job_id": str(job.id)}, exc_info=True)

    def _build_subtitles(
        self,
        scenes: List[dict[str, Any]],
        words_per_batch: int | None = None,
        style: dict[str, Any] | None = None,
    ) -> str:
        if not scenes:
            return "1\n00:00:00,000 --> 00:00:05,000\nВидео появится скоро.\n"
        lines: List[str] = []
        cursor = 0.0
        counter = 1
        batch_size = max(1, words_per_batch or 0) if words_per_batch else None
        uppercase = bool(style.get("uppercase")) if style else False
        for scene in scenes:
            duration = float(scene.get("duration_seconds") or 5)
            text = (scene.get("voiceover") or scene.get("focus") or "").strip()
            if uppercase:
                text = text.upper()
            if batch_size and text:
                words = text.split()
                if not words:
                    start = self._format_timestamp(cursor)
                    end = self._format_timestamp(cursor + duration)
                    lines.append(f"{counter}\n{start} --> {end}\n{text}\n")
                    counter += 1
                else:
                    chunks = [words[i : i + batch_size] for i in range(0, len(words), batch_size)]
                    total_chunks = len(chunks)
                    segment = duration / total_chunks if total_chunks and duration > 0 else 0
                    for chunk_idx, chunk in enumerate(chunks):
                        start_ts = (
                            self._format_timestamp(cursor + segment * chunk_idx)
                            if segment
                            else self._format_timestamp(cursor)
                        )
                        end_ts = (
                            self._format_timestamp(cursor + segment * (chunk_idx + 1))
                            if segment
                            else self._format_timestamp(cursor + duration)
                        )
                        chunk_text = " ".join(chunk)
                        if uppercase:
                            chunk_text = chunk_text.upper()
                        lines.append(f"{counter}\n{start_ts} --> {end_ts}\n{chunk_text}\n")
                        counter += 1
            else:
                start = self._format_timestamp(cursor)
                end = self._format_timestamp(cursor + duration)
                lines.append(f"{counter}\n{start} --> {end}\n{text}\n")
                counter += 1
            cursor += duration
        return "\n".join(lines)

    def _parse_timestamp(self, value: str) -> float:
        match = re.match(r"(\d{2}):(\d{2}):(\d{2}),(\d{3})", value.strip())
        if not match:
            return 0.0
        hours, minutes, seconds, millis = map(int, match.groups())
        return hours * 3600 + minutes * 60 + seconds + millis / 1000.0

    def _rebatch_subtitles(
        self,
        text: str,
        batch_size: int,
        style_payload: Optional[SubtitleStyleRequest] = None,
    ) -> str:
        entries = self._parse_srt_entries(text)
        if not entries:
            return text
        batch_size = max(1, batch_size)
        counter = 1
        blocks: list[str] = []
        uppercase = bool(style_payload.uppercase) if style_payload else False
        for start, end, content in entries:
            words = content.split()
            if not words:
                block = f"{counter}\n{self._format_timestamp(start)} --> {self._format_timestamp(end)}\n{content}\n"
                blocks.append(block)
                counter += 1
                continue
            chunks = [words[i : i + batch_size] for i in range(0, len(words), batch_size)]
            duration = max(end - start, 0.0)
            lengths = [len(" ".join(chunk)) or 1 for chunk in chunks]
            total_len = sum(lengths) or 1
            elapsed = start
            for idx, (chunk_len, chunk) in enumerate(zip(lengths, chunks)):
                if duration > 0:
                    if idx == len(chunks) - 1:
                        portion = end - elapsed
                    else:
                        portion = (chunk_len / total_len) * duration
                else:
                    portion = 0
                chunk_start = elapsed
                chunk_end = elapsed + portion
                elapsed = chunk_end
                chunk_text = " ".join(chunk)
                if uppercase:
                    chunk_text = chunk_text.upper()
                block = (
                    f"{counter}\n"
                    f"{self._format_timestamp(chunk_start)} --> {self._format_timestamp(chunk_end)}\n"
                    f"{chunk_text}\n"
                )
                blocks.append(block)
                counter += 1
        return "\n".join(blocks)

    def _parse_srt_entries(self, text: str) -> list[tuple[float, float, str]]:
        entries: list[tuple[float, float, str]] = []
        blocks = re.split(r"\n\s*\n", text.strip())
        for block in blocks:
            lines = [line.strip() for line in block.strip().splitlines() if line.strip()]
            if len(lines) < 2:
                continue
            time_line = lines[1]
            match = re.match(r"(\d{2}:\d{2}:\d{2},\d{3})\s*-->\s*(\d{2}:\d{2}:\d{2},\d{3})", time_line)
            if not match:
                continue
            start = self._parse_timestamp(match.group(1))
            end = self._parse_timestamp(match.group(2))
            content = " ".join(lines[2:])
            entries.append((start, end, content))
        return entries

    def _ffmpeg_force_style(self, style: dict[str, Any] | None) -> str | None:
        if not style:
            return None
        parts = []
        font = style.get("font_family")
        if font:
            parts.append(f"Fontname={font}")
        font_size = style.get("font_size")
        if font_size:
            parts.append(f"Fontsize={font_size}")
        color = style.get("color")
        if color:
            parts.append(f"PrimaryColour={self._ass_color(color)}")
        outline_color = style.get("outline_color")
        if outline_color:
            parts.append(f"OutlineColour={self._ass_color(outline_color)}")
        bold = style.get("bold")
        if bold is not None:
            parts.append(f"Bold={'-1' if bold else '0'}")
        margin_bottom = style.get("margin_bottom")
        if margin_bottom is not None:
            parts.append(f"MarginV={margin_bottom}")
        return ",".join(parts) if parts else None

    def _ass_color(self, value: str) -> str:
        hex_value = value.lstrip("#")
        if len(hex_value) != 6:
            return "&H00FFFFFF"
        r = hex_value[0:2]
        g = hex_value[2:4]
        b = hex_value[4:6]
        return f"&H00{b}{g}{r}"

    def _format_timestamp(self, seconds: float) -> str:
        total_ms = int(max(0, seconds) * 1000)
        hours = total_ms // 3_600_000
        minutes = (total_ms % 3_600_000) // 60_000
        secs = (total_ms % 60_000) // 1000
        millis = total_ms % 1000
        return f"{hours:02}:{minutes:02}:{secs:02},{millis:03}"

    def _select_backgrounds(self, job: VideoJob, storyboard: dict[str, Any]) -> list[str]:
        auto_urls: list[str] = []
        prefixes: list[str] = []
        if job.template_id:
            prefixes.append(job.template_id)
        prefixes.append(self.settings.default_background_folder)
        for prefix in prefixes:
            prefix_path = self._compose_media_prefix(prefix)
            if not prefix_path:
                continue
            try:
                objects = self.storage.list_files(prefix_path)
            except ValueError:
                continue
            urls = [
                obj.get("url")
                for obj in objects
                if obj.get("url") and obj.get("key") and self._is_image_asset(obj["key"])
            ]
            if urls:
                auto_urls = urls
                break
        if not auto_urls:
            key = (job.template_id or "").lower()
            auto_urls = BACKGROUND_LIBRARY.get(key) or BACKGROUND_LIBRARY.get("test") or []
        return auto_urls

    def _get_audio_duration(self, audio_bytes: bytes | None) -> float | None:
        if not audio_bytes:
            return None
        try:
            with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
                tmp.write(audio_bytes)
                tmp_path = tmp.name
            clip = AudioFileClip(tmp_path)
            duration = clip.duration
            clip.close()
            os.remove(tmp_path)
            return duration
        except Exception:
            return None

    def _find_artifact(self, job: VideoJob, kind: str) -> VideoJobArtifact | None:
        for artifact in job.artifacts:
            if artifact.kind == kind:
                return artifact
        return None

    def _load_voiceover_audio(self, job: VideoJob) -> bytes | None:
        artifact = self._find_artifact(job, "voiceover")
        if not artifact or not artifact.path:
            return None
        try:
            return self.storage.download_bytes(artifact.path)
        except ValueError:
            return None

    def _load_subtitles_text(self, job: VideoJob) -> str:
        if job.subtitles_text:
            return job.subtitles_text
        artifact = self._find_artifact(job, "subtitles")
        if artifact and artifact.path:
            try:
                return self.storage.download_bytes(artifact.path).decode("utf-8")
            except ValueError:
                return ""
        return ""
    def _render_video(
        self,
        job: VideoJob,
        storyboard: dict[str, Any],
        backgrounds: list[str],
        audio_bytes: bytes | None,
        subtitles_text: str | None,
        audio_duration: float | None,
    ) -> bytes | None:
        scenes = storyboard.get("scenes") or []
        if not scenes:
            scenes = [{"duration_seconds": job.duration_seconds, "title": job.idea}]
        try:
            self.log.info(
                "rendering video",
                extra={
                    "job_id": str(job.id),
                    "scenes": len(scenes),
                    "backgrounds": len(backgrounds),
                    "has_audio": bool(audio_bytes),
                    "has_subtitles": bool(subtitles_text),
                },
            )
            with tempfile.TemporaryDirectory() as tmpdir:
                clips = []
                for idx, scene in enumerate(scenes):
                    duration = max(1, int(scene.get("duration_seconds") or 5))
                    preferred_url = scene.get("background_url")
                    bg_path = self._download_background(backgrounds, idx, tmpdir, preferred_url)
                    if bg_path:
                        clip = ImageClip(bg_path).set_duration(duration)
                    else:
                        clip = ColorClip(size=(1080, 1920), color=(0, 0, 0)).set_duration(duration)
                    clip = clip.resize(newsize=(1080, 1920)).set_position("center")
                    clips.append(clip)
                video_clip = concatenate_videoclips(clips, method="compose")
                total_duration = video_clip.duration
                video_clip = video_clip.set_fps(24)
                audio_clips: list[Any] = []
                final_audio_clip = None
                voice_clip = None
                if audio_bytes:
                    audio_path = os.path.join(tmpdir, "voiceover.mp3")
                    with open(audio_path, "wb") as f:
                        f.write(audio_bytes)
                    voice_clip = AudioFileClip(audio_path)
                    audio_clips.append(voice_clip)
                    if audio_duration:
                        total_duration = audio_duration
                soundtrack_clip = None
                soundtrack_path, _ = self._prepare_soundtrack_file(job, tmpdir)
                if soundtrack_path:
                    try:
                        soundtrack_clip = AudioFileClip(soundtrack_path)
                        audio_clips.append(soundtrack_clip)
                    except Exception as exc:  # pragma: no cover
                        self.log.warning(
                            "soundtrack clip load failed",
                            extra={"job_id": str(job.id)},
                            exc_info=exc,
                        )
                        soundtrack_clip = None
                target_duration = total_duration or video_clip.duration
                audio_layers = []
                if voice_clip:
                    audio_layers.append(voice_clip if not target_duration else voice_clip.subclip(0, target_duration))
                if soundtrack_clip:
                    music_layer = soundtrack_clip
                    if target_duration:
                        music_layer = music_layer.subclip(0, target_duration)
                    volume = 0.35 if voice_clip else 0.6
                    music_layer = music_layer.volumex(volume)
                    audio_layers.append(music_layer)
                if audio_layers:
                    if len(audio_layers) == 1:
                        final_audio_clip = audio_layers[0]
                    else:
                        final_audio_clip = CompositeAudioClip(audio_layers)
                    video_clip = video_clip.set_audio(final_audio_clip)
                if target_duration and target_duration > 0:
                    video_clip = video_clip.subclip(0, target_duration)
                output_path = os.path.join(tmpdir, "final.mp4")
                video_clip.write_videofile(
                    output_path,
                    fps=24,
                    codec="libx264",
                    audio_codec="aac",
                    verbose=True,
                    logger="bar",
                    ffmpeg_params=["-pix_fmt", "yuv420p"],
                )
                for clip in clips:
                    clip.close()
                if final_audio_clip:
                    try:
                        final_audio_clip.close()
                    except Exception:  # pragma: no cover
                        pass
                for resource in audio_clips:
                    try:
                        resource.close()
                    except Exception:  # pragma: no cover
                        pass
                video_clip.close()
                final_path = output_path
                if subtitles_text:
                    subs_path = os.path.join(tmpdir, "subs.srt")
                    with open(subs_path, "w", encoding="utf-8") as f:
                        f.write(subtitles_text)
                    burned_path = os.path.join(tmpdir, "final_with_subs.mp4")
                    subs_posix = pathlib.Path(subs_path).as_posix()
                    style_arg = self._ffmpeg_force_style(job.subtitle_style)
                    vf = f"subtitles='{subs_posix}'"
                    if style_arg:
                        vf += f":force_style='{style_arg}'"
                    cmd = [
                        "ffmpeg",
                        "-y",
                        "-i",
                        final_path,
                        "-vf",
                        vf,
                        "-c:v",
                        "libx264",
                        "-pix_fmt",
                        "yuv420p",
                        "-c:a",
                        "copy",
                        burned_path,
                    ]
                    try:
                        subprocess.run(cmd, check=True)
                        final_path = burned_path
                    except subprocess.CalledProcessError as exc:
                        self.log.warning(
                            "ffmpeg burn-in failed",
                            extra={"job_id": str(job.id)},
                            exc_info=exc,
                        )
                with open(final_path, "rb") as f:
                    return f.read()
        except Exception as exc:  # pragma: no cover
            self.log.warning("video render failed", extra={"job_id": str(job.id)}, exc_info=exc)
        return None

    def _download_background(
        self,
        backgrounds: list[str],
        idx: int,
        tmpdir: str,
        preferred_url: str | None = None,
    ) -> str | None:
        url_candidates: list[str] = []
        if preferred_url:
            url_candidates.append(preferred_url)
        if backgrounds:
            url_candidates.append(backgrounds[idx % len(backgrounds)])
        for url in url_candidates:
            if not url:
                continue
            try:
                resp = httpx.get(url, timeout=30.0)
                resp.raise_for_status()
                suffix = pathlib.Path(url).suffix or ".png"
                path = os.path.join(tmpdir, f"background_{idx}{suffix}")
                with open(path, "wb") as f:
                    f.write(resp.content)
                return path
            except Exception:  # pragma: no cover
                continue
        return None

    def _match_music_catalog(self, name: str | None) -> dict[str, str] | None:
        if not name:
            return None
        target = name.strip().lower()
        if not target:
            return None
        for entry in self.music_catalog:
            entry_name = (entry.get("name") or "").strip().lower()
            if entry_name == target:
                return entry
        return None

    def _resolve_soundtrack_source(self, job: VideoJob) -> tuple[str | None, dict[str, Any]]:
        candidate = (job.soundtrack_url or "").strip()
        if candidate:
            source = "custom_url"
            if not candidate.lower().startswith(("http://", "https://")):
                source = "storage_key"
            return candidate, {"source": source}
        catalog_entry = self._match_music_catalog(job.soundtrack)
        if catalog_entry:
            catalog_url = (catalog_entry.get("low_volume") or catalog_entry.get("url") or "").strip()
            if catalog_url:
                meta: dict[str, Any] = {
                    "source": "music_catalog",
                    "name": catalog_entry.get("name"),
                }
                if catalog_entry.get("low_volume"):
                    meta["variant"] = "low_volume"
                return catalog_url, meta
        preset_value = (job.soundtrack or "").strip()
        if preset_value.lower().startswith(("http://", "https://")):
            return preset_value, {"source": "preset_url"}
        return None, {}

    def _prepare_soundtrack_file(self, job: VideoJob, tmpdir: str) -> tuple[str | None, dict[str, Any]]:
        resolved_value, meta = self._resolve_soundtrack_source(job)
        if not resolved_value:
            return None, meta
        candidate = resolved_value.strip()
        if candidate.lower().startswith(("http://", "https://")):
            try:
                resp = httpx.get(candidate, timeout=30.0)
                resp.raise_for_status()
                suffix = pathlib.Path(candidate).suffix or ".mp3"
                path = os.path.join(tmpdir, f"soundtrack{suffix}")
                with open(path, "wb") as f:
                    f.write(resp.content)
                return path, meta
            except Exception as exc:  # pragma: no cover - network best effort
                self.log.warning(
                    "soundtrack download failed",
                    extra={"job_id": str(job.id), "soundtrack_url": candidate},
                    exc_info=exc,
                )
                return None, meta
        try:
            data = self.storage.download_bytes(candidate)
        except ValueError as exc:  # pragma: no cover
            self.log.warning(
                "soundtrack storage fetch failed",
                extra={"job_id": str(job.id), "key": candidate},
                exc_info=exc,
            )
            return None, meta
        suffix = pathlib.Path(candidate).suffix or ".mp3"
        path = os.path.join(tmpdir, f"soundtrack{suffix}")
        with open(path, "wb") as f:
            f.write(data)
        return path, meta

    def _merge_storyboard(self, job: VideoJob, payload: DraftApprovalRequest) -> dict[str, Any]:
        scenes_input = payload.scenes or []
        if not scenes_input and not job.storyboard:
            raise ValueError("no scenes provided for storyboard approval")
        scenes: list[dict[str, Any]] = []
        source = scenes_input or job.storyboard or []
        for idx, scene in enumerate(source, start=1):
            if hasattr(scene, "model_dump"):
                data = scene.model_dump(exclude_none=True)  # type: ignore[attr-defined]
            else:
                data = dict(scene)
            data["position"] = data.get("position") or idx
            if not data.get("duration_seconds"):
                data["duration_seconds"] = 5
            scenes.append(data)
        summary = payload.summary or job.storyboard_summary or job.idea
        return {"summary": summary, "scenes": scenes}
