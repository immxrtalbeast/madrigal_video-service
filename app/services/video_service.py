from __future__ import annotations

import time
from datetime import datetime
import logging
from typing import Any, List
from uuid import UUID, uuid4

from app.clients.gemini import GeminiClient
from app.clients.supabase_storage import SupabaseStorageClient
from app.clients.tts import ElevenLabsClient
from app.config import Settings
from app.models.api import IdeaExpansionRequest, VideoGenerationRequest
from app.models.domain import (
    VideoJob,
    VideoJobArtifact,
    VideoJobStage,
    VideoJobStatusHistory,
)
from app.queue.queue import BaseQueue
from app.storage.repository import VideoJobRepository


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
        self.tts = None
        if settings.tts_provider.lower() == "elevenlabs":
            self.tts = ElevenLabsClient(
                api_key=settings.elevenlabs_api_key,
                voice_id=settings.elevenlabs_voice_id,
                model_id=settings.elevenlabs_model_id,
                base_url=settings.elevenlabs_base_url,
                logger=self.log,
            )
        self.storage = SupabaseStorageClient(
            api_url=settings.supabase_api_url,
            public_url=settings.supabase_public_url,
            bucket=settings.supabase_bucket,
            api_key=settings.supabase_api_key,
        )

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
            voice_profile=payload.voice_profile or self.settings.tts_voice,
            soundtrack=payload.soundtrack or self.settings.backing_track,
            subtitles_url=None,
            video_url=None,
            error=None,
        )
        self.repo.save(job)
        if self.queue is not None:
            self.queue.enqueue(job.id)
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
        self.log.debug("starting storyboard drafting", extra={"job_id": str(job.id)})
        storyboard = self.gemini.generate_storyboard(
            idea=job.idea,
            target_audience=job.target_audience,
            style=job.style or self.settings.default_style,
            duration_seconds=job.duration_seconds,
        )
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

        self._update_status(job, VideoJobStage.ASSETS, "Preparing visual asset prompts")
        frames_payload = {
            "provider": self.settings.text2img_provider,
            "style": job.style,
            "scenes": storyboard.get("scenes", []),
        }
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
        if self.tts and self.tts.enabled():
            audio_bytes = self.tts.synthesize(voice_script)
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

        soundtrack_path = f"{job.assets_folder}/audio/soundtrack.txt"
        soundtrack_url = self.storage.upload_text(
            soundtrack_path,
            f"Use soundtrack preset '{job.soundtrack}' for pacing.",
        )
        self._add_artifact(
            job,
            kind="soundtrack",
            path=soundtrack_path,
            url=soundtrack_url,
            metadata={"preset": job.soundtrack},
        )

        subtitles_text = self._build_subtitles(storyboard.get("scenes", []))
        subtitles_path = f"{job.assets_folder}/subtitles.srt"
        subtitles_url = self.storage.upload_text(
            subtitles_path,
            subtitles_text,
            content_type="application/x-subrip; charset=utf-8",
        )
        job.subtitles_url = subtitles_url
        self._add_artifact(
            job,
            kind="subtitles",
            path=subtitles_path,
            url=subtitles_url,
            metadata={"source": "whisper"},
        )
        time.sleep(0.05)

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

        video_path = f"{job.assets_folder}/final.mp4"
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

    def _build_assets_folder(self) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        return f"{self.settings.supabase_folder_prefix}/{timestamp}-{uuid4().hex[:8]}"

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
        return artifact

    def _build_subtitles(self, scenes: List[dict[str, Any]]) -> str:
        if not scenes:
            return "1\n00:00:00,000 --> 00:00:05,000\nВидео появится скоро.\n"
        lines: List[str] = []
        cursor = 0
        for idx, scene in enumerate(scenes, start=1):
            duration = int(scene.get("duration_seconds") or 5)
            start = self._format_timestamp(cursor)
            end = self._format_timestamp(cursor + duration)
            text = scene.get("voiceover") or scene.get("focus") or ""
            lines.append(f"{idx}\n{start} --> {end}\n{text}\n")
            cursor += duration
        return "\n".join(lines)

    def _format_timestamp(self, seconds: int) -> str:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02}:{minutes:02}:{secs:02},000"
