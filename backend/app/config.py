from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_host: str = Field(default="0.0.0.0", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    cors_origins: str = Field(default="http://localhost:5173", alias="CORS_ORIGINS")

    max_upload_mb: int = Field(default=100, alias="MAX_UPLOAD_MB")
    upload_dir: str = Field(default="./tmp_uploads", alias="UPLOAD_DIR")

    overlap_threshold: float = Field(default=0.30, alias="OVERLAP_THRESHOLD")
    confidence_threshold: float = Field(default=0.60, alias="CONFIDENCE_THRESHOLD")
    borderline_low: float = Field(default=0.40, alias="BORDERLINE_LOW")
    borderline_high: float = Field(default=0.60, alias="BORDERLINE_HIGH")
    min_name_similarity: float = Field(default=0.65, alias="MIN_NAME_SIMILARITY")
    min_intersection_size: int = Field(default=5, alias="MIN_INTERSECTION_SIZE")
    min_distinct_values: int = Field(default=8, alias="MIN_DISTINCT_VALUES")
    max_relationships_per_table_pair: int = Field(default=8, alias="MAX_RELATIONSHIPS_PER_TABLE_PAIR")

    neo4j_uri: str = Field(default="", alias="NEO4J_URI")
    neo4j_user: str = Field(default="neo4j", alias="NEO4J_USER")
    neo4j_password: str = Field(default="", alias="NEO4J_PASSWORD")

    groq_api_key: str = Field(default="", alias="GROQ_API_KEY")
    groq_model: str = Field(default="llama-3.3-70b-versatile", alias="GROQ_MODEL")

    rate_limit_uploads_per_minute: int = Field(default=10, alias="RATE_LIMIT_UPLOADS_PER_MINUTE")

    @property
    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
