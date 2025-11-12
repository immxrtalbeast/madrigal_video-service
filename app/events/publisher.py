from __future__ import annotations

import json
import logging
from typing import Any

try:  # pragma: no cover - optional dependency
    from kafka import KafkaProducer
except ImportError:  # pragma: no cover - fallback when kafka-python absent
    KafkaProducer = None  # type: ignore

from app.models.domain import VideoJob


class JobEventPublisher:
    """Publishes job snapshots to Kafka so downstream services can react in real time."""

    def __init__(self, bootstrap_servers: str, topic: str, logger: logging.Logger | None = None) -> None:
        if KafkaProducer is None:
            raise RuntimeError("kafka-python is not installed")
        if not bootstrap_servers:
            raise ValueError("bootstrap_servers is required")
        if not topic:
            raise ValueError("topic is required")
        self._topic = topic
        self._logger = logger or logging.getLogger(__name__)
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda payload: json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            linger_ms=5,
        )

    def publish_job(self, job: VideoJob, extras: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {"job": job.model_dump(mode="json")}
        if extras:
            payload.update(extras)
        try:
            self._producer.send(self._topic, payload)
        except Exception:
            self._logger.warning(
                "failed to publish job event",
                extra={"job_id": str(job.id), "topic": self._topic},
                exc_info=True,
            )

    def close(self) -> None:
        try:
            self._producer.flush()
            self._producer.close()
        except Exception:
            self._logger.debug("job event publisher close failed", exc_info=True)
