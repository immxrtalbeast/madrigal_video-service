from __future__ import annotations

import json
import logging
import math
import os
import pathlib
import subprocess
import tempfile
import time
from datetime import datetime
from typing import Any, Callable, List, Optional
from uuid import UUID, uuid4

import httpx
import numpy as np
from PIL import Image
from moviepy.audio.AudioClip import AudioArrayClip, concatenate_audioclips
from moviepy.editor import (
    AudioFileClip,
    ColorClip,
    CompositeAudioClip,
    ImageClip,
    VideoFileClip,
    concatenate_videoclips,
)
from moviepy.video.fx import all as vfx

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
    SceneAudio,
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
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/images/07d47b1443cf8ec_big.jpg",
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/images/d3b67dbf5b0f4547a7d9d1704f1e5ecf.jpg",
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/images/dadwa288da21dap.png",
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/images/dwadzxczf1ff31fasf.png",
        "https://bc16f399-f374-4a1e-a578-8a4052cc8a91.selstorage.ru/assets/shared/test/images/xczsidaid124.png",
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

    def create_job(self, payload: VideoGenerationRequest, user_id: str) -> VideoJob:
        assets_folder = self._build_assets_folder(user_id)
        job = VideoJob(
            id=uuid4(),
            user_id=user_id,
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
            scene_audio=[],
            soundtrack=payload.soundtrack or self.settings.backing_track,
            soundtrack_url=payload.soundtrack_url,
            background_video_url=(payload.background_video_url.strip() if payload.background_video_url else None),
            subtitles_url=None,
            subtitles_text=None,
            subtitle_batch_size=None,
            subtitle_style=None,
            video_url=None,
            error=None,
        )
        self._save_job(job)
        if self.queue is not None:
            self.queue.enqueue(job.id)
        self._emit_job_update(job)
        return job

    def get_job(self, job_id: UUID, user_id: str | None = None) -> VideoJob:
        job = self.repo.get(job_id)
        if not job:
            if user_id:
                job = self._load_job_from_storage(user_id, job_id)
                if job:
                    self._save_job(job)
            if not job:
                raise ValueError("Video job not found")
        if user_id and job.user_id != user_id:
            raise ValueError("Video job not found")
        return job

    def list_jobs(self) -> list[VideoJob]:
        return self.repo.list()

    def list_user_jobs(self, user_id: str) -> list[VideoJob]:
        jobs = self.repo.list()
        filtered = [job for job in jobs if job.user_id == user_id]
        if not filtered:
            filtered = self._load_jobs_from_storage(user_id)
        filtered.sort(key=lambda job: job.created_at, reverse=True)
        return filtered

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
        scenes = storyboard.get("scenes") or []
        max_scenes = max(1, self.settings.max_scenes or 1)
        if scenes and len(scenes) > max_scenes:
            storyboard["scenes"] = scenes = scenes[:max_scenes]
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

    def approve_draft(self, job_id: UUID, payload: DraftApprovalRequest, user_id: str) -> VideoJob:
        job = self.get_job(job_id, user_id)
        if job.stage != VideoJobStage.DRAFT_REVIEW:
            raise ValueError("draft stage is not awaiting approval")
        storyboard = self._merge_storyboard(job, payload)
        scenes = storyboard.get("scenes", [])
        max_scenes = max(1, self.settings.max_scenes or 1)
        if scenes and len(scenes) > max_scenes:
            scenes = scenes[:max_scenes]
            storyboard["scenes"] = scenes
        job.storyboard = scenes
        job.storyboard_summary = storyboard.get("summary")
        if payload.background_video_url is not None:
            sanitized = payload.background_video_url.strip()
            job.background_video_url = sanitized or None
        storyboard_path = f"{job.assets_folder}/storyboard.json"
        self.storage.upload_json(storyboard_path, storyboard)
        self._update_status(job, VideoJobStage.DRAFT_REVIEW, "Storyboard approved. Queued for asset build-out")
        if self.queue is not None:
            self.queue.enqueue(job.id)
        else:  # pragma: no cover - fallback for misconfiguration
            self._run_post_draft(job, storyboard)
        return job

    def _run_post_draft(self, job: VideoJob, storyboard: dict[str, Any]) -> None:
        selected_backgrounds: list[str] = []
        if not (job.background_video_url and job.background_video_url.strip()):
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

        combined_voiceover = self._generate_scene_audio_assets(job, storyboard)
        scenes_for_voice = storyboard.get("scenes", []) or []
        has_full_audio = (
            combined_voiceover
            and job.scene_audio
            and len(job.scene_audio) >= len(scenes_for_voice) > 0
            and all(record.path for record in job.scene_audio)
        )
        if has_full_audio:
            voice_audio_path = f"{job.assets_folder}/audio/voiceover.mp3"
            voice_audio_url = self.storage.upload_bytes(
                voice_audio_path,
                combined_voiceover,
                content_type="audio/mpeg",
            )
            self._add_or_replace_artifact(
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
            self._add_or_replace_artifact(
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
        auto_subtitles, used_transcripts = self._combine_scene_subtitles(job, storyboard.get("scenes", []))
        subtitle_source = "auto"
        if auto_subtitles:
            subtitles_text = auto_subtitles
            subtitle_source = "whisper" if used_transcripts else "auto"
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
            metadata={"source": subtitle_source},
        )
        time.sleep(0.05)

        self._update_status(job, VideoJobStage.SUBTITLE_REVIEW, "Awaiting subtitle approval")

    def approve_subtitles(self, job_id: UUID, payload: SubtitlesApprovalRequest, user_id: str) -> VideoJob:
        job = self.get_job(job_id, user_id)
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
        self._save_job(job)
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
        job_output_path = f"jobs/{self._normalize_storage_segment(job.user_id)}/{job.id}/final.mp4"
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
            video_url = self.storage.upload_bytes(job_output_path, video_bytes, content_type="video/mp4")
        else:
            video_url = self.storage.upload_bytes(job_output_path, b"", content_type="video/mp4")
        job.video_url = video_url
        self._add_artifact(
            job,
            kind="video",
            path=job_output_path,
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
        self._save_job(job)
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
        user_assets = self._list_user_media(folder, user_id, predicate=None)
        shared_assets = self.list_shared_media(folder)
        return self._merge_assets(user_assets, shared_assets)

    def list_media_videos(self, folder: str | None, user_id: str) -> list[MediaAsset]:
        user_assets = self._list_user_media(folder, user_id, predicate=self._is_video_asset)
        shared_assets = self.list_shared_videos(folder)
        return self._merge_assets(user_assets, shared_assets)

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

    def list_shared_videos(self, folder: str | None = None) -> list[MediaAsset]:
        prefix = self._compose_shared_prefix(folder)
        try:
            objects = self.storage.list_files(prefix)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        assets: list[MediaAsset] = []
        for obj in objects:
            key = obj.get("key")
            if not key or not self._is_video_asset(key):
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

    def _list_user_media(
        self,
        folder: str | None,
        user_id: str,
        predicate: Callable[[str], bool] | None = None,
    ) -> list[MediaAsset]:
        prefix = self._user_media_prefix(folder or "", user_id)
        try:
            objects = self.storage.list_files(prefix)
        except ValueError as exc:
            raise ValueError(str(exc)) from exc
        assets: list[MediaAsset] = []
        checker = predicate or self._is_image_asset
        for obj in objects:
            key = obj.get("key")
            if not key or key.rstrip().endswith("/"):
                continue
            if checker and not checker(key):
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

    def _merge_assets(self, primary: list[MediaAsset], secondary: list[MediaAsset]) -> list[MediaAsset]:
        result = list(primary)
        seen = {asset.key for asset in primary}
        for asset in secondary:
            if asset.key in seen:
                continue
            result.append(asset)
            seen.add(asset.key)
        return result

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
                }
            )
        return items

    def _build_assets_folder(self, user_id: str) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        prefix = self.settings.storage_folder_prefix.strip("/")
        user_segment = self._normalize_storage_segment(user_id)
        unique = f"{timestamp}-{uuid4().hex[:8]}"
        parts = [segment for segment in (prefix, user_segment, unique) if segment]
        return "/".join(parts)

    def _compose_media_prefix(self, folder: str | None) -> str:
        folder_segment = self._normalize_storage_segment(folder)
        root_segment = self._normalize_storage_segment(self.settings.media_root_prefix)
        if folder_segment and root_segment and folder_segment.startswith(root_segment):
            return folder_segment
        parts = [segment for segment in (root_segment, folder_segment) if segment]
        return "/".join(parts)

    def _compose_job_prefix(self, user_id: str) -> str:
        jobs_root = "jobs"
        user_segment = self._normalize_storage_segment(user_id)
        return "/".join(segment for segment in (jobs_root, user_segment) if segment)

    def _job_status_path(self, user_id: str, job_id: UUID) -> str:
        return "/".join(
            segment for segment in ("jobs", self._normalize_storage_segment(user_id), str(job_id), "status.json") if segment
        )

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

    def _is_video_asset(self, key: str) -> bool:
        if not key or key.rstrip().endswith("/"):
            return False
        lowered = key.lower()
        return lowered.endswith((".mp4", ".mov", ".webm", ".mkv", ".avi"))

    def _save_job(self, job: VideoJob) -> None:
        self.repo.save(job)
        try:
            self._persist_job_snapshot(job)
        except Exception as exc:  # pragma: no cover - storage best effort
            self.log.warning(
                "job snapshot persist failed",
                extra={"job_id": str(job.id), "error": str(exc)},
            )

    def _persist_job_snapshot(self, job: VideoJob) -> None:
        payload = job.model_dump(mode="json")
        path = self._job_status_path(job.user_id, job.id)
        self.storage.upload_json(path, payload)

    def _load_job_from_storage(self, user_id: str, job_id: UUID) -> VideoJob | None:
        path = self._job_status_path(user_id, job_id)
        try:
            raw = self.storage.download_bytes(path)
        except ValueError:
            return None
        try:
            payload = json.loads(raw.decode("utf-8"))
            return VideoJob.model_validate(payload)
        except Exception:  # pragma: no cover - corrupt snapshot
            self.log.warning("job snapshot parse failed", extra={"job_id": str(job_id)}, exc_info=True)
            return None

    def _load_jobs_from_storage(self, user_id: str) -> list[VideoJob]:
        prefix = self._compose_job_prefix(user_id)
        try:
            objects = self.storage.list_files(prefix)
        except ValueError as exc:
            self.log.warning("job snapshot list failed", extra={"user_id": user_id, "error": str(exc)})
            return []
        jobs: list[VideoJob] = []
        for obj in objects:
            key = obj.get("key") or ""
            if not key.endswith("status.json"):
                continue
            try:
                raw = self.storage.download_bytes(key)
            except ValueError:
                continue
            try:
                payload = json.loads(raw.decode("utf-8"))
                job = VideoJob.model_validate(payload)
            except Exception:  # pragma: no cover
                self.log.warning("job snapshot parse failed", extra={"key": key}, exc_info=True)
                continue
            self.repo.save(job)
            jobs.append(job)
        return jobs

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
        self._save_job(job)
        self._emit_job_update(job)
        return artifact

    def _add_or_replace_artifact(
        self,
        job: VideoJob,
        kind: str,
        path: str,
        url: str,
        metadata: dict[str, object] | None = None,
    ) -> VideoJobArtifact:
        existing = self._find_artifact(job, kind)
        if existing:
            existing.path = path
            existing.url = url
            existing.metadata = metadata or {}
            self._save_job(job)
            self._emit_job_update(job)
            return existing
        return self._add_artifact(job, kind, path, url, metadata)

    def _emit_job_update(self, job: VideoJob) -> None:
        if not self.events:
            return
        try:
            self.events.publish_job(job)
        except Exception:  # pragma: no cover
            self.log.warning("job event emission failed", extra={"job_id": str(job.id)}, exc_info=True)

    def _generate_scene_audio_assets(self, job: VideoJob, storyboard: dict[str, Any]) -> bytes | None:
        scenes: list[dict[str, Any]] = storyboard.get("scenes") or []
        max_scenes = max(1, self.settings.max_scenes or 1)
        if scenes and len(scenes) > max_scenes:
            storyboard["scenes"] = scenes = scenes[:max_scenes]
        job.scene_audio = []
        job.artifacts = [artifact for artifact in job.artifacts if artifact.kind != "voiceover_scene"]
        combined_audio = bytearray()
        total_duration = 0.0
        for idx, scene in enumerate(scenes):
            record, audio_bytes = self._synthesize_scene_audio(job, scene, idx)
            if record and record.path and record.url:
                scene_duration = record.duration or self._estimate_duration(scene.get("voiceover") or "")
                job.scene_audio.append(record)
                if audio_bytes:
                    combined_audio.extend(audio_bytes)
            else:
                scene_duration = self._estimate_duration(scene.get("voiceover") or "")
                job.scene_audio.append(
                    SceneAudio(
                        scene_index=idx,
                        path=None,
                        url=None,
                        duration=scene_duration,
                        subtitles=None,
                    )
                )
            scene["duration_seconds"] = max(1, int(math.ceil(scene_duration)))
            scene["audio_duration"] = scene_duration
            if record and record.url:
                scene["audio_url"] = record.url
            total_duration += scene_duration
        if total_duration > 0:
            job.duration_seconds = int(math.ceil(total_duration))
        self._save_job(job)
        return bytes(combined_audio) if combined_audio else None

    def _synthesize_scene_audio(
        self,
        job: VideoJob,
        scene: dict[str, Any],
        index: int,
    ) -> tuple[SceneAudio | None, bytes | None]:
        text = self._scene_voiceover_text(scene, index, job)
        if not text or not self.tts or not self.tts.enabled():
            return None, None
        attempts = max(1, self.settings.tts_scene_retry_count or 1)
        audio_bytes: bytes | None = None
        for attempt in range(attempts):
            try:
                audio_bytes = self.tts.synthesize(text, voice_id=job.voice_id)
                break
            except Exception as exc:  # pragma: no cover - external API
                self.log.warning(
                    "scene tts failed",
                    extra={"job_id": str(job.id), "scene": index + 1, "attempt": attempt + 1},
                    exc_info=True,
                )
                time.sleep(1)
        if not audio_bytes:
            return None, None
        audio_path = f"{job.assets_folder}/audio/scene-{index + 1}.mp3"
        audio_url = self.storage.upload_bytes(
            audio_path,
            audio_bytes,
            content_type="audio/mpeg",
        )
        duration = self._get_audio_duration(audio_bytes) or self._estimate_duration(text)
        self._add_artifact(
            job,
            kind="voiceover_scene",
            path=audio_path,
            url=audio_url,
            metadata={
                "scene": index + 1,
                "duration": duration,
                "voice": job.voice_profile,
            },
        )
        subtitles: str | None = None
        if self.whisper and self.whisper.enabled():
            try:
                subtitles = self.whisper.transcribe(audio_bytes, filename=f"scene-{index + 1}.mp3")
            except Exception:  # pragma: no cover - whisper failure
                subtitles = None
        return SceneAudio(
            scene_index=index,
            path=audio_path,
            url=audio_url,
            duration=duration or 0.0,
            subtitles=subtitles,
        ), audio_bytes

    def _scene_voiceover_text(self, scene: dict[str, Any], index: int, job: VideoJob) -> str:
        text = (scene.get("voiceover") or scene.get("focus") or "").strip()
        if text:
            return text
        return f"Сцена {index + 1}. {job.idea}"

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

    def _combine_scene_subtitles(
        self,
        job: VideoJob,
        scenes: List[dict[str, Any]],
    ) -> tuple[str | None, bool]:
        if not job.scene_audio:
            return None, False
        entries: list[tuple[float, float, str]] = []
        cursor = 0.0
        used_transcripts = False
        for record in sorted(job.scene_audio, key=lambda x: x.scene_index):
            scene = scenes[record.scene_index] if record.scene_index < len(scenes) else {}
            duration = record.duration or float(scene.get("duration_seconds") or 0)
            if record.subtitles:
                segments = self._parse_srt_entries(record.subtitles)
                if segments:
                    used_transcripts = True
                    for start, end, content in segments:
                        entries.append((cursor + start, cursor + end, content))
                else:
                    text = self._scene_voiceover_text(scene, record.scene_index, job)
                    entries.append((cursor, cursor + max(duration, 0.5), text))
            else:
                text = self._scene_voiceover_text(scene, record.scene_index, job)
                entries.append((cursor, cursor + max(duration, 0.5), text))
            cursor += max(duration, 0.5)
        if not entries:
            return None, False
        lines = []
        for idx, (start, end, content) in enumerate(entries, start=1):
            lines.append(
                f"{idx}\n{self._format_timestamp(start)} --> {self._format_timestamp(end)}\n{content.strip()}\n"
            )
        return "\n".join(lines), used_transcripts

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

    def _build_scene_voice_clip(self, job: VideoJob, tmpdir: str):
        if not job.scene_audio:
            return None, None
        clips = []
        for record in sorted(job.scene_audio, key=lambda x: x.scene_index):
            if not record.path:
                duration = max(record.duration, 0.5)
                samples = max(int(duration * 44100), 1)
                silent_array = np.zeros((samples, 1), dtype=np.float32)
                clips.append(AudioArrayClip(silent_array, fps=44100))
                continue
            try:
                audio_bytes = self.storage.download_bytes(record.path)
            except ValueError:
                continue
            if not audio_bytes:
                continue
            audio_path = os.path.join(tmpdir, f"scene-audio-{record.scene_index + 1}.mp3")
            with open(audio_path, "wb") as f:
                f.write(audio_bytes)
            try:
                clip = AudioFileClip(audio_path)
            except Exception:  # pragma: no cover
                continue
            clips.append(clip)
        if not clips:
            return None, None
        voice_clip = concatenate_audioclips(clips)
        return voice_clip, voice_clip.duration

    def _estimate_duration(self, text: str) -> float:
        words = max(len(text.split()), 1)
        wpm = 150
        return max(1.5, (words / max(wpm, 1)) * 60.0)

    def _find_artifact(self, job: VideoJob, kind: str) -> VideoJobArtifact | None:
        for artifact in job.artifacts:
            if artifact.kind == kind:
                return artifact
        return None

    def _load_voiceover_audio(self, job: VideoJob) -> bytes | None:
        artifact = self._find_artifact(job, "voiceover")
        if not artifact or not artifact.path:
            return None
        if not artifact.path.lower().endswith((".mp3", ".wav", ".ogg")):
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
                scene_durations: list[int] = []
                for scene in scenes:
                    scene_duration = max(1, int(scene.get("duration_seconds") or 5))
                    scene_durations.append(scene_duration)
                if not scene_durations:
                    scene_durations = [max(1, job.duration_seconds or 5)]
                requested_duration = sum(scene_durations)

                background_video_clip = None
                background_source = job.background_video_url.strip() if job.background_video_url else ""
                if background_source:
                    background_video_clip = self._build_background_video_clip(
                        background_source,
                        tmpdir,
                        requested_duration,
                        job_id=job.id,
                    )
                    if background_video_clip:
                        backgrounds = []

                clips = []
                if background_video_clip:
                    video_clip = background_video_clip
                else:
                    for idx, (scene, duration) in enumerate(zip(scenes, scene_durations)):
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
                if job.scene_audio:
                    voice_clip, inferred_duration = self._build_scene_voice_clip(job, tmpdir)
                    if voice_clip:
                        audio_clips.append(voice_clip)
                        if inferred_duration:
                            total_duration = inferred_duration
                elif audio_bytes:
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
                    volume = 0.12 if voice_clip else 0.3
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

    def _download_video_asset(self, source: str, tmpdir: str, job_id: UUID | None = None) -> str | None:
        if not source:
            return None
        candidate = source.strip()
        if not candidate:
            return None
        if candidate.lower().startswith(("http://", "https://")):
            try:
                read_timeout = max(10.0, float(getattr(self.settings, "asset_download_timeout", 60.0)))
                timeout = httpx.Timeout(
                    connect=10.0,
                    read=read_timeout,
                    write=10.0,
                    pool=read_timeout,
                )
                with httpx.stream("GET", candidate, timeout=timeout) as resp:
                    resp.raise_for_status()
                    suffix = pathlib.Path(candidate).suffix or ".mp4"
                    path = os.path.join(tmpdir, f"background_video{suffix}")
                    with open(path, "wb") as f:
                        for chunk in resp.iter_bytes():
                            if chunk:
                                f.write(chunk)
                return path
            except Exception as exc:  # pragma: no cover
                self.log.warning(
                    "background video download failed",
                    extra={"job_id": str(job_id) if job_id else None, "background_video_url": candidate},
                    exc_info=exc,
                )
                return None
        try:
            data = self.storage.download_bytes(candidate)
        except ValueError as exc:  # pragma: no cover
            self.log.warning(
                "background video storage fetch failed",
                extra={"job_id": str(job_id) if job_id else None, "key": candidate},
                exc_info=exc,
            )
            return None
        suffix = pathlib.Path(candidate).suffix or ".mp4"
        path = os.path.join(tmpdir, f"background_video{suffix}")
        with open(path, "wb") as f:
            f.write(data)
        return path

    def _build_background_video_clip(
        self,
        source: str,
        tmpdir: str,
        target_duration: int | float,
        job_id: UUID | None = None,
    ) -> VideoFileClip | None:
        video_path = self._download_video_asset(source, tmpdir, job_id=job_id)
        if not video_path:
            return None
        try:
            clip = VideoFileClip(video_path).resize(newsize=(1080, 1920)).without_audio()
        except Exception as exc:  # pragma: no cover
            self.log.warning(
                "background video load failed",
                extra={"background_video_url": source},
                exc_info=exc,
            )
            return None
        if target_duration:
            if clip.duration >= target_duration:
                return clip.subclip(0, target_duration)
            return clip.fx(vfx.loop, duration=target_duration)
        return clip

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
            catalog_url = (catalog_entry.get("url") or "").strip()
            if catalog_url:
                meta: dict[str, Any] = {
                    "source": "music_catalog",
                    "name": catalog_entry.get("name"),
                }
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
                base_timeout = max(10.0, float(getattr(self.settings, "asset_download_timeout", 60.0)))
                timeout = httpx.Timeout(
                    connect=min(10.0, base_timeout),
                    read=base_timeout,
                    write=min(10.0, base_timeout),
                    pool=None,
                )
                with httpx.stream("GET", candidate, timeout=timeout) as resp:
                    resp.raise_for_status()
                    suffix = pathlib.Path(candidate).suffix or ".mp3"
                    path = os.path.join(tmpdir, f"soundtrack{suffix}")
                    with open(path, "wb") as f:
                        for chunk in resp.iter_bytes():
                            if chunk:
                                f.write(chunk)
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
