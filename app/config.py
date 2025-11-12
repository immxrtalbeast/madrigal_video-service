from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VIDEO_SERVICE_", env_file=".env", env_file_encoding="utf-8")

    app_name: str = "video-service"
    host: str = "0.0.0.0"
    port: int = 8100

    default_language: str = "ru"
    default_style: str = "cinematic"
    default_tone: str = "дружелюбный"

    kafka_enabled: bool = False
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "video_jobs"
    kafka_updates_topic: str = "video_updates"
    kafka_group_id: str = "video-service-consumer"

    # Object storage configuration
    s3_endpoint_url: str = ""
    s3_region: str | None = None
    s3_public_url: str = ""
    s3_bucket: str = "generated-videos"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    storage_folder_prefix: str = "jobs"
    media_root_prefix: str = ""
    default_background_folder: str = "assets/test"
    shared_media_prefix: str = "assets/shared"

    # Generation provider preferences
    text2img_provider: str = "imagen"
    tts_provider: str = "google-tts"
    tts_voice: str = "ru-RU-Standard-C"
    backing_track: str = "viral-beat"
    elevenlabs_api_key: str = ""
    elevenlabs_voice_id: str = ""
    elevenlabs_model_id: str = "eleven_multilingual_v2"
    elevenlabs_base_url: str = "https://api.elevenlabs.io"
    whisper_local_model: str = "base"

    gemini_api_key: str = ""
    gemini_model: str = "models/gemini-pro"
    voice_catalog: list[dict[str, str]] = Field(default_factory=list)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
