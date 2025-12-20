import pytest
import pytest_asyncio
from dotenv import load_dotenv
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlalchemy.orm import Session as SyncSession
from sqlalchemy.orm.session import SessionTransaction

from db.engine import create_engine, get_database_url

load_dotenv()


@pytest.fixture(scope="session")
def db_init_script_path() -> str:
    return "sql_scripts/db_init.sql"


@pytest.fixture(scope="session")
def db_drop_script_path() -> str:
    return "sql_scripts/db_drop.sql"


@pytest.fixture(scope="session")
def engine():
    url = get_database_url()
    return create_engine(url, is_echo=True)


@pytest_asyncio.fixture(scope="session", autouse=True)
async def prepare_database(
    engine: AsyncEngine, db_init_script_path, db_drop_script_path
):
    """Создаём таблицы перед тестами и удаляем после."""
    async with engine.begin() as conn:
        with open(db_init_script_path, "r") as f:
            await conn.exec_driver_sql(f.read())
    yield

    async with engine.begin() as conn:
        with open(db_drop_script_path, "r") as f:
            await conn.exec_driver_sql(f.read())


async_session_factory = async_sessionmaker(engine, expire_on_commit=False)  # type: ignore


@pytest_asyncio.fixture
async def session(engine: AsyncEngine):
    async with engine.connect() as conn:
        trans = await conn.begin()

        Session = async_session_factory

        async with Session(bind=conn) as s:
            await s.begin_nested()

            @event.listens_for(s.sync_session, "after_transaction_end")
            def restart_savepoint(
                sync_sess: SyncSession, transaction: SessionTransaction
            ):
                if not transaction.nested:
                    return
                if sync_sess.is_active and sync_sess.get_transaction() is not None:
                    return
                sync_sess.begin_nested()

            try:
                yield s
            finally:
                await trans.rollback()
