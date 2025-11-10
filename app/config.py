from functools import lru_cache

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
    kafka_group_id: str = "video-service-consumer"

    # Supabase storage (S3-compatible) configuration
    supabase_public_url: str = "https://example.supabase.co/storage/v1/object/public"
    supabase_bucket: str = "generated-videos"
    supabase_folder_prefix: str = "jobs"
    supabase_api_url: str = "https://example.supabase.co"
    supabase_api_key: str = ""

    # Generation provider preferences
    text2img_provider: str = "imagen"
    tts_provider: str = "google-tts"
    tts_voice: str = "ru-RU-Standard-C"
    backing_track: str = "viral-beat"
    elevenlabs_api_key: str = ""
    elevenlabs_voice_id: str = ""
    elevenlabs_model_id: str = "eleven_multilingual_v2"
    elevenlabs_base_url: str = "https://api.elevenlabs.io"

    gemini_api_key: str = ""
    gemini_model: str = "models/gemini-pro"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
