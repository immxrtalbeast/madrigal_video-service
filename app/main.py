from __future__ import annotations

import base64
import binascii
import logging
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Query, status

from app.config import Settings, get_settings
from app.models.api import (
    DraftApprovalRequest,
    IdeaExpansionRequest,
    IdeaExpansionResponse,
    MediaListResponse,
    MediaUploadRequest,
    MediaUploadResponse,
    SubtitlesApprovalRequest,
    VoiceListResponse,
    VideoGenerationRequest,
    VideoJobListResponse,
    VideoJobResponse,
)
from app.queue.queue import KafkaQueue, LocalQueue
from app.services.video_service import VideoService
from app.storage.repository import VideoJobRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

app = FastAPI()

_repo = VideoJobRepository()
_service: VideoService | None = None


def require_user_id(x_user_id: str = Header(default=None, alias="X-User-ID")) -> str:
    if not x_user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="X-User-ID header required")
    return x_user_id


def get_video_service(settings: Settings = Depends(get_settings)) -> VideoService:
    global _service
    if _service is None:
        service = VideoService(repo=_repo, settings=settings)
        queue = _build_queue(settings, service)
        service.bind_queue(queue)
        _service = service
    return _service


def _build_queue(settings: Settings, service: VideoService):
    if settings.kafka_enabled:
        return KafkaQueue(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic=settings.kafka_topic,
            group_id=settings.kafka_group_id,
            processor=service.process_job,
        )
    return LocalQueue(processor=service.process_job)


@app.post("/videos", response_model=VideoJobResponse, status_code=status.HTTP_202_ACCEPTED)
def create_video(
    payload: VideoGenerationRequest,
    service: VideoService = Depends(get_video_service),
) -> VideoJobResponse:
    job = service.create_job(payload)
    return VideoJobResponse(job=job)


@app.get("/videos", response_model=VideoJobListResponse)
def list_videos(service: VideoService = Depends(get_video_service)) -> VideoJobListResponse:
    return VideoJobListResponse(items=service.list_jobs())


@app.get("/videos/{job_id}", response_model=VideoJobResponse)
def get_video(job_id: UUID, service: VideoService = Depends(get_video_service)) -> VideoJobResponse:
    try:
        job = service.get_job(job_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return VideoJobResponse(job=job)


@app.post("/ideas:expand", response_model=IdeaExpansionResponse)
def expand_idea(
    payload: IdeaExpansionRequest,
    service: VideoService = Depends(get_video_service),
) -> IdeaExpansionResponse:
    result = service.expand_idea(payload)
    return IdeaExpansionResponse(**result)


@app.post("/videos/{job_id}/draft:approve", response_model=VideoJobResponse)
def approve_draft(
    job_id: UUID,
    payload: DraftApprovalRequest,
    service: VideoService = Depends(get_video_service),
) -> VideoJobResponse:
    try:
        job = service.approve_draft(job_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return VideoJobResponse(job=job)


@app.post("/videos/{job_id}/subtitles:approve", response_model=VideoJobResponse)
def approve_subtitles(
    job_id: UUID,
    payload: SubtitlesApprovalRequest,
    service: VideoService = Depends(get_video_service),
) -> VideoJobResponse:
    try:
        job = service.approve_subtitles(job_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return VideoJobResponse(job=job)


@app.post("/media", response_model=MediaUploadResponse, status_code=status.HTTP_201_CREATED)
def upload_media(
    payload: MediaUploadRequest,
    user_id: str = Depends(require_user_id),
    service: VideoService = Depends(get_video_service),
) -> MediaUploadResponse:
    try:
        data = base64.b64decode(payload.data, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="invalid base64 payload") from exc
    try:
        asset = service.upload_media(
            folder=payload.folder,
            filename=payload.filename,
            data=data,
            user_id=user_id,
            content_type=payload.content_type,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return MediaUploadResponse(asset=asset)


@app.get("/media", response_model=MediaListResponse)
def list_media(
    folder: str | None = Query(
        default=None,
        description="Путь внутри бакета (например assets/test). Оставьте пустым для корня.",
    ),
    user_id: str = Depends(require_user_id),
    service: VideoService = Depends(get_video_service),
) -> MediaListResponse:
    try:
        items = service.list_media(folder, user_id=user_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return MediaListResponse(items=items)


@app.get("/media/shared", response_model=MediaListResponse)
def list_shared_media(
    folder: str | None = Query(
        default=None,
        description="Папка в каталоге общих ассетов (например test)",
    ),
    service: VideoService = Depends(get_video_service),
) -> MediaListResponse:
    try:
        items = service.list_shared_media(folder)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return MediaListResponse(items=items)


@app.get("/voices", response_model=VoiceListResponse)
def list_voices(service: VideoService = Depends(get_video_service)) -> VoiceListResponse:
    items = service.list_voices()
    return VoiceListResponse(items=items)
