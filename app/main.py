from __future__ import annotations

from uuid import UUID

import logging

from fastapi import Depends, FastAPI, HTTPException, status

from app.config import Settings, get_settings
from app.models.api import (
    DraftApprovalRequest,
    IdeaExpansionRequest,
    IdeaExpansionResponse,
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
