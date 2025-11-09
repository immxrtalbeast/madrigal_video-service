from __future__ import annotations

from threading import Lock
from typing import Dict, List
from uuid import UUID

from app.models.domain import VideoJob


class VideoJobRepository:
    def __init__(self) -> None:
        self._jobs: Dict[UUID, VideoJob] = {}
        self._lock = Lock()

    def save(self, job: VideoJob) -> VideoJob:
        with self._lock:
            self._jobs[job.id] = job
        return job

    def get(self, job_id: UUID) -> VideoJob | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return job.model_copy(deep=True) if job else None

    def list(self) -> List[VideoJob]:
        with self._lock:
            return [job.model_copy(deep=True) for job in self._jobs.values()]
