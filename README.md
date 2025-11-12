# Video Service

## Описание
Сервис отвечает за полный цикл генерации вертикальных видео: принимает заявки от фронта (через API Gateway), оркеструет этапы `queued → drafting → assets → audio → rendering → ready`, складывает все артефакты в S3-совместимое хранилище (SelStorage/S3) и возвращает статус/ссылки пользователю. На этапе Drafting используется Gemini для сториборда; стадия Audio умеет синтезировать голос через ElevenLabs и сохранять mp3 в бакете; остальные шаги готовят промпты/инструкции для визуалов, музыки, субтитров и финального рендера.

## Основные возможности
- `POST /videos` — создание задачи, запуск сценарного этапа и мгновенный возврат `job_id`.
- `POST /videos/{id}/draft:approve` — принять (или отредактировать) сториборд, после чего пайплайн продолжает работу (ассеты → озвучка → субтитры).
- `POST /videos/{id}/subtitles:approve` — внести правки в автоматически сгенерированные субтитры и запустить финальный рендер.
- `GET /videos` / `/videos/{id}` — отслеживание статуса, получение истории стадий и ссылок на артефакты (storyboard, frames, voiceover, subtitles, manifest, final.mp4).
- `POST /ideas:expand` — краткое расширение идеи (тон, формат, аудитория) для фронтовых подсказок.
- `POST /media` — загрузка пользовательских изображений/видео (base64) прямо в бакет; `GET /media?folder=backgrounds` — список файлов пользователя. Media автоматически складываются под `assets/{user_id}/…`, так что автор видит только свои объекты (заголовок `X-User-ID` прокидывает API Gateway).
- `GET /media/shared?folder=test` — доступ к общим ассетам (статичные бэкграунды, стоки), расположенным под `shared_media_prefix`.
- Очередь (Kafka или встроенная in-memory) для воркеров, чтобы отделить HTTP от долгих операций.

## Технологии
- Python 3.11, FastAPI, Pydantic.
- Kafka / локальная очередь.
- Gemini API (storyboard), ElevenLabs TTS (аудио), локальный Whisper (openai-whisper + ffmpeg) для субтитров, S3-совместимое хранилище (SelStorage/boto3) для артефактов.
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
- `KAFKA_ENABLED`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_GROUP_ID`, `KAFKA_UPDATES_TOPIC`.
- `S3_ENDPOINT_URL`, `S3_REGION`, `S3_PUBLIC_URL`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `STORAGE_FOLDER_PREFIX`, `MEDIA_ROOT_PREFIX`, `DEFAULT_BACKGROUND_FOLDER`.
- `GEMINI_API_KEY`, `GEMINI_MODEL`.
- `TEXT2IMG_PROVIDER`, `TTS_PROVIDER`, `TTS_VOICE`, `BACKING_TRACK`.
- `ELEVENLABS_API_KEY`, `ELEVENLABS_VOICE_ID`, `ELEVENLABS_MODEL_ID`, `ELEVENLABS_BASE_URL` (нужны при `TTS_PROVIDER=elevenlabs`).
- `WHISPER_LOCAL_MODEL` — название локальной модели (например, `base`, `small`). Нужны зависимости `openai-whisper` и `ffmpeg`.
Все значения можно описать в `video-service/.env` и подключить через `env_file` в docker-compose.
