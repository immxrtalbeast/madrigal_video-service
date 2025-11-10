# Video Service

## Описание
Сервис отвечает за полный цикл генерации вертикальных видео: принимает заявки от фронта (через API Gateway), оркеструет этапы `queued → drafting → assets → audio → rendering → ready`, сохраняет все промежуточные артефакты в Supabase и возвращает статус/ссылки пользователю. На этапе Drafting используется Gemini для сториборда; стадия Audio умеет синтезировать голос через ElevenLabs и складывать mp3 в Supabase; остальные шаги готовят промпты/инструкции для визуалов, музыки, субтитров и финального рендера.

## Основные возможности
- `POST /videos` — создание задачи, запуск конвейера и мгновенный возврат `job_id`.
- `GET /videos` / `/videos/{id}` — отслеживание статуса, получение истории стадий и ссылок на артефакты (storyboard, frames, voiceover, subtitles, manifest, final.mp4).
- `POST /ideas:expand` — краткое расширение идеи (тон, формат, аудитория) для фронтовых подсказок.
- Очередь (Kafka или встроенная in-memory) для воркеров, чтобы отделить HTTP от долгих операций.

## Технологии
- Python 3.11, FastAPI, Pydantic.
- Kafka / локальная очередь.
- Gemini API (storyboard), ElevenLabs TTS (аудио), локальный Whisper (openai-whisper + ffmpeg) для субтитров, Supabase Storage (артефакты).
- httpx для внешних вызовов, pytest для тестов.

## TODO
1. Подключить реальные генераторы визуалов (text2img/стоки) и музыку, добавить готовые библиотеки ассетов.
2. Вынести рендер в отдельный воркер с FFmpeg/MoviePy и обновлением `final.mp4` после сборки.
3. Перенести `VideoJob` в постоянное хранилище (Postgres), добавить ретраи и WebSocket/вебхуки статусов.
4. Настроить метрики/логирование (Prometheus, Sentry) и лимиты на внешний API.

## Установка и запуск
```bash
cd video-service
poetry install      # или python -m venv / pip install -r requirements
poetry run uvicorn app.main:app --reload --port 8100
# для продакшена используем docker-compose (service video-service) или uvicorn без reload
```
Для режима с Kafka общий `docker-compose` (поднимет Zookeeper + Kafka) и выстав `VIDEO_SERVICE_KAFKA_ENABLED=true`.

## Переменные окружения (`VIDEO_SERVICE_*`)
- `HOST`, `PORT`, `DEFAULT_LANGUAGE`, `DEFAULT_STYLE`, `DEFAULT_TONE`.
- `KAFKA_ENABLED`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`.
- `SUPABASE_API_URL`, `SUPABASE_API_KEY` (service role), `SUPABASE_PUBLIC_URL`, `SUPABASE_BUCKET`, `SUPABASE_FOLDER_PREFIX`.
- `GEMINI_API_KEY`, `GEMINI_MODEL`.
- `TEXT2IMG_PROVIDER`, `TTS_PROVIDER`, `TTS_VOICE`, `BACKING_TRACK`.
- `ELEVENLABS_API_KEY`, `ELEVENLABS_VOICE_ID`, `ELEVENLABS_MODEL_ID`, `ELEVENLABS_BASE_URL` (нужны при `TTS_PROVIDER=elevenlabs`).
- `WHISPER_LOCAL_MODEL` — название локальной модели (например, `base`, `small`). Нужны зависимости `openai-whisper` и `ffmpeg`.
Все значения можно описать в `video-service/.env` и подключить через `env_file` в docker-compose.
