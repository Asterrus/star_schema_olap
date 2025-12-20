import pytest
from sqlalchemy import text


class TestCreateTables:
    @pytest.mark.asyncio
    async def test_create_tables_success(self, session):
        """Проверяем что фикстура создания таблиц отработала и таблица Customers создана."""
        res = await session.execute(text("SELECT * FROM Customers"))
        assert res
