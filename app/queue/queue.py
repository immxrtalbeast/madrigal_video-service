from __future__ import annotations

import json
import threading
import time
from queue import Queue
from typing import Callable
from uuid import UUID

try:
    from kafka import KafkaConsumer, KafkaProducer
except ImportError:  # pragma: no cover - optional dependency
    KafkaConsumer = None  # type: ignore
    KafkaProducer = None  # type: ignore


class BaseQueue:
    def enqueue(self, job_id: UUID) -> None: ...  # pragma: no cover


class LocalQueue(BaseQueue):
    def __init__(self, processor: Callable[[UUID], None]) -> None:
        self._processor = processor
        self._queue: Queue[UUID] = Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def enqueue(self, job_id: UUID) -> None:
        self._queue.put(job_id)

    def _run(self) -> None:
        while True:
            job_id = self._queue.get()
            try:
                self._processor(job_id)
            finally:
                self._queue.task_done()


class KafkaQueue(BaseQueue):
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        processor: Callable[[UUID], None],
    ) -> None:
        if KafkaProducer is None or KafkaConsumer is None:
            raise RuntimeError("kafka-python is not installed")
        self._topic = topic
        self._processor = processor
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        )
        self._consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )
        self._thread = threading.Thread(target=self._consume, daemon=True)
        self._thread.start()

    def enqueue(self, job_id: UUID) -> None:
        payload = {"job_id": str(job_id), "ts": time.time()}
        self._producer.send(self._topic, payload)
        self._producer.flush()

    def _consume(self) -> None:
        for message in self._consumer:
            job_id = UUID(message.value["job_id"])
            try:
                self._processor(job_id)
            except Exception:  # pragma: no cover - best effort logging
                continue
