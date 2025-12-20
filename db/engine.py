import os

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    create_async_engine,
)


def get_database_url() -> str:
    postgres_user = os.getenv("POSTGRES_USER")
    postgres_password = os.getenv("POSTGRES_PASSWORD")
    postgres_db = os.getenv("POSTGRES_DB")
    postgres_host = os.getenv("POSTGRES_HOST")
    postgres_port = os.getenv("POSTGRES_PORT")
    assert all(
        [postgres_user, postgres_password, postgres_db, postgres_host, postgres_port]
    ), "Missing required database environment variables"
    return f"postgresql+psycopg://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"


def create_engine(url: str, is_echo: bool = True) -> AsyncEngine:
    return create_async_engine(
        url,
        echo=is_echo,
    )
