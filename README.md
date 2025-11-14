# Video Service

## Описание
Сервис отвечает за полный цикл генерации вертикальных видео: принимает заявки от фронта (через API Gateway), оркеструет этапы `queued → drafting → assets → audio → rendering → ready`, складывает все артефакты в S3-совместимое хранилище (SelStorage/S3) и возвращает статус/ссылки пользователю. На этапе Drafting используется Gemini для сториборда; стадия Audio умеет синтезировать голос через ElevenLabs и сохранять mp3 в бакете; остальные шаги готовят промпты/инструкции для визуалов, музыки, субтитров и финального рендера.

## Основные возможности
- `POST /videos` — создание задачи, запуск сценарного этапа и мгновенный возврат `job_id`.
- `POST /videos/{id}/draft:approve` — принять (или отредактировать) сториборд, после чего пайплайн продолжает работу (ассеты → озвучка → субтитры).
- `POST /videos/{id}/subtitles:approve` — внести правки в автоматически сгенерированные субтитры и запустить финальный рендер. Можно указать `words_per_batch`, чтобы автоматически разбить текст на 1–N слов в строке, а также объект `style` (`font_family`, `font_size`, `color`, `outline_color`, `bold`, `uppercase`, `margin_bottom`), который прокидывается в ffmpeg `force_style` для кастомизации.
- `GET /videos` / `/videos/{id}` — отслеживание статуса, получение истории стадий и ссылок на артефакты (storyboard, frames, voiceover, subtitles, manifest, final.mp4).
- `POST /ideas:expand` — краткое расширение идеи (тон, формат, аудитория) для фронтовых подсказок.
- `POST /media` — загрузка пользовательских изображений/видео (base64) прямо в бакет; `GET /media?folder=backgrounds` вернёт объединённый список: сначала ваши файлы из `assets/{user_id}/…`, затем общий каталог (shared). Отдельные запросы к `/media/shared` оставлены для совместимости, но фронту достаточно одного вызова.
- `POST /media/videos`, `GET /media/videos` — те же принципы для mp4/webm: ответ включает и личные, и shared-ролики. Для больших файлов можно использовать `POST /media/videos:upload` (multipart: `folder`, опционально `filename`, `file=@clip.mp4`).
- `GET /voices` — список доступных голосов (с id, описанием и ссылками для предпрослушки). `POST /videos` принимает `voice_id`, который будет использован для TTS.
- `GET /music` — каталог фоновых треков (название, описание, автор, `url`). При создании видео можно передать `soundtrack` (имя из каталога) либо `soundtrack_url` (прямая ссылка/ключ на mp3). Во время финального рендера сервис скачает дорожку, приглушит её и смешает с голосом.
- Каждый storyboard режется на отдельные сцены (максимум 10): для каждой сцены генерируем собственную озвучку (ElevenLabs), вычисляем фактическую длительность, а субтитры/рендер синхронизируются именно с этими mp3. Если TTS недоступен, включается fallback с оценкой длительности.
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
- `S3_ADDRESSING_STYLE` (virtual/path), `SHARED_MEDIA_PREFIX`, `VOICE_CATALOG` (список голосов в формате JSON; если не задан, используется `ELEVENLABS_VOICE_ID` как дефолт).
- `MUSIC_CATALOG` — JSON-массив с объектами `{ "name": "...", "description": "...", "author": "...", "url": "https://…/audio.mp3" }`. Сервис автоматически приглушает дорожку при микшировании, поэтому отдельный `low_volume` больше не нужен.
- `ASSET_DOWNLOAD_TIMEOUT` — лимит (в секундах) на скачивание фоновой музыки/ассетов. По умолчанию 120 с; увеличьте, если треки хостятся на медленных CDN.
- `MISTRAL_API_KEY`, `MISTRAL_MODEL`, `MISTRAL_BASE_URL` — для запасного провайдера сториборда (если Gemini возвращает 503, сервис автоматически попробует Mistral).
- `GEMINI_API_KEY`, `GEMINI_MODEL`.
- `TEXT2IMG_PROVIDER`, `TTS_PROVIDER`, `TTS_VOICE`, `BACKING_TRACK`.
- `ELEVENLABS_API_KEY`, `ELEVENLABS_VOICE_ID`, `ELEVENLABS_MODEL_ID`, `ELEVENLABS_BASE_URL` (нужны при `TTS_PROVIDER=elevenlabs`).
- `WHISPER_LOCAL_MODEL` — название локальной модели (например, `base`, `small`). Нужны зависимости `openai-whisper` и `ffmpeg`.
- `MAX_SCENES` (по умолчанию 10) — ограничение на количество сцен в сториборде. Всё, что больше, обрезается.
- `TTS_SCENE_RETRY_COUNT` (по умолчанию 3) — сколько раз повторять вызов ElevenLabs для каждой сцены перед fallback.
Все значения можно описать в `video-service/.env` и подключить через `env_file` в docker-compose.

### Музыка и фоновые дорожки
- `POST /videos` понимает поля:
  - `soundtrack` — название из `/music`.
  - `soundtrack_url` — произвольная ссылка на mp3 (например, результат `POST /media`). Можно также передать ключ в бакете (`assets/user123/audio/demo.mp3`) — сервис сам выкачает байты через S3 API.
- Если указаны оба поля, приоритет у `soundtrack_url`. При его недоступности сервис попытается подобрать дорожку по имени (`soundtrack` → `/music`). Если ничего не найдено, видео будет без фоновой музыки.
- Во время рендера MoviePy/FFmpeg микшируют озвучку и фон: голос остаётся на 100%, музыка приглушается (0.35 по громкости при наличии голоса или 0.6, если озвучки нет).

### Фоновые видео
- `POST /media/videos` / `GET /media/videos` — загрузка и выбор видеороликов (mp4/webm/mov), которые можно использовать в качестве единственного фона на протяжении всего финального ролика. Дополнительно можно воспользоваться `POST /media/videos:upload` (multipart). `GET /media/shared/videos` оставлен для совместимости, но базовый `/media/videos` уже возвращает и shared-контент, и пользовательский.
- `background_video_url` в `POST /videos` и `POST /videos/{id}/draft:approve` — дополнительное поле, которым можно указать URL/ключ видеофона. Если оно задано, пайплайн не подбирает картинки и не монтирует сцены по отдельности: берётся один ролик, который обрезается/зацикливается под длительность сценария, после чего поверх кладутся озвучка, музыка и субтитры.
