from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class VideoJobStage(str, Enum):
    QUEUED = "queued"
    DRAFTING = "drafting"
    DRAFT_REVIEW = "draft_review"
    ASSETS = "assets"
    AUDIO = "audio"
    SUBTITLE_REVIEW = "subtitle_review"
    RENDERING = "rendering"
    READY = "ready"
    FAILED = "failed"


class VideoJobStatusHistory(BaseModel):
    status: str
    stage: VideoJobStage
    message: str
    occurred_at: datetime = Field(default_factory=datetime.utcnow)


class VideoJobArtifact(BaseModel):
    kind: str
    path: str
    url: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None


class SceneAudio(BaseModel):
    scene_index: int
    path: Optional[str]
    url: Optional[str]
    duration: float
    subtitles: Optional[str] = None


class VideoJob(BaseModel):
    id: UUID
    idea: str
    language: str
    duration_seconds: int
    style: Optional[str]
    target_audience: Optional[str]
    template_id: Optional[str]
    status: str
    stage: VideoJobStage
    status_history: List[VideoJobStatusHistory] = Field(default_factory=list)
    assets_folder: str
    artifacts: List[VideoJobArtifact] = Field(default_factory=list)
    storyboard_summary: Optional[str]
    voice_profile: Optional[str]
    voice_id: Optional[str]
    scene_audio: List[SceneAudio] = Field(default_factory=list)
    subtitle_batch_size: Optional[int] = None
    subtitle_style: Optional[dict[str, Any]] = None
    soundtrack: Optional[str]
    soundtrack_url: Optional[str]
    background_video_url: Optional[str] = None
    subtitles_url: Optional[str]
    subtitles_text: Optional[str]
    video_url: Optional[str]
    error: Optional[str]
    storyboard: Optional[List[dict[str, Any]]] = Field(default=None)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
