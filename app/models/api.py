from __future__ import annotations

from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict, Field, validator

from .domain import VideoJob, VideoJobStage


class VideoGenerationRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    idea: str = Field(..., validation_alias="idea")
    duration_seconds: Optional[int] = Field(default=45, ge=15, le=300, validation_alias="duration_seconds")
    language: Optional[str] = Field(default=None, validation_alias="language")
    style: Optional[str] = Field(default=None, validation_alias="style")
    target_audience: Optional[str] = Field(default=None, validation_alias="target_audience")
    template_id: Optional[str] = Field(default=None, validation_alias="template_id")
    constraints: Optional[List[str]] = Field(default=None, validation_alias="constraints")
    source_video_url: Optional[str] = Field(default=None, validation_alias="source_video_url")
    aspect_ratio: Optional[str] = Field(default="9:16", validation_alias="aspect_ratio")
    voice_profile: Optional[str] = Field(default=None, validation_alias="voice_profile")
    voice_id: Optional[str] = Field(default=None, validation_alias="voice_id")
    soundtrack: Optional[str] = Field(default=None, validation_alias="soundtrack")
    soundtrack_url: Optional[str] = Field(default=None, validation_alias="soundtrack_url")
    storyboard_ref: Optional[str] = Field(default=None, validation_alias="storyboard_ref")

    @validator("idea")
    def validate_idea(cls, value: str) -> str:  # noqa: D417
        if len(value.strip()) < 5:
            raise ValueError("idea must contain at least 5 characters")
        return value


class VideoJobResponse(BaseModel):
    job: VideoJob


class VideoJobListResponse(BaseModel):
    items: List[VideoJob]


class ScenePayload(BaseModel):
    position: Optional[int] = None
    title: Optional[str] = None
    focus: Optional[str] = None
    voiceover: Optional[str] = None
    visual: Optional[str] = None
    duration_seconds: Optional[int] = None
    background_url: Optional[str] = None


class DraftApprovalRequest(BaseModel):
    summary: Optional[str] = None
    scenes: List[ScenePayload]


class IdeaExpansionRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    idea: str = Field(..., validation_alias="idea")
    tone: Optional[str] = Field(default=None, validation_alias="tone")
    language: Optional[str] = Field(default=None, validation_alias="language")
    target_audience: Optional[str] = Field(default=None, validation_alias="target_audience")
    duration_seconds: Optional[int] = Field(default=None, validation_alias="duration_seconds")
    output_format: Optional[str] = Field(default=None, validation_alias="output_format")

    @validator("idea")
    def validate_expand_idea(cls, value: str) -> str:  # noqa: D417
        if len(value.strip()) < 5:
            raise ValueError("idea must contain at least 5 characters")
        return value


class IdeaExpansionResponse(BaseModel):
    summary: str


class MediaAsset(BaseModel):
    key: str
    url: str
    size: Optional[int] = None
    last_modified: Optional[datetime] = None


class MediaListResponse(BaseModel):
    items: List[MediaAsset]


class MediaUploadResponse(BaseModel):
    asset: MediaAsset


class MediaUploadRequest(BaseModel):
    folder: str
    filename: Optional[str] = None
    content_type: Optional[str] = None
    data: str


class SubtitlesApprovalRequest(BaseModel):
    text: Optional[str] = None
    words_per_batch: Optional[int] = Field(default=None, ge=1, le=10, validation_alias="words_per_batch")
    style: Optional["SubtitleStyleRequest"] = None


class SubtitleStyleRequest(BaseModel):
    font_family: Optional[str] = None
    font_size: Optional[int] = Field(default=None, ge=8, le=120)
    color: Optional[str] = Field(default=None, description="Colors in #RRGGBB")
    outline_color: Optional[str] = Field(default=None)
    bold: Optional[bool] = None
    uppercase: Optional[bool] = None
    margin_bottom: Optional[int] = Field(default=None, ge=0, le=400)


class VoiceInfo(BaseModel):
    voice_id: str
    name: Optional[str] = None
    description: Optional[str] = None
    preview_url: Optional[str] = None


class VoiceListResponse(BaseModel):
    items: List[VoiceInfo]


SubtitlesApprovalRequest.model_rebuild()


class MusicInfo(BaseModel):
    name: str
    description: Optional[str] = None
    author: Optional[str] = None
    url: str
    low_volume: Optional[str] = None


class MusicListResponse(BaseModel):
    items: List[MusicInfo]
