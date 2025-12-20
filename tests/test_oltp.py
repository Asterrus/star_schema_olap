from datetime import date
from uuid import UUID

import pytest
from sqlalchemy import text

from sql_scripts.oltp import (
    insert_customer,
    insert_product,
    insert_sale,
    update_customer,
    update_product,
)


class TestOLTP:
    """Тесты вставки/обновления записей в основных таблицах OLTP"""

    @pytest.mark.asyncio
    async def test_insert_product_success(self, session):
        product_id = await insert_product(session, "Product 1", "Category 1")
        assert isinstance(product_id, UUID)
        res = await session.execute(text("SELECT count(*) FROM Products"))
        assert res.scalar() == 1

    @pytest.mark.asyncio
    async def test_update_product_success(self, session):
        product_id = await insert_product(session, "Product 1", "Category 1")
        await update_product(session, product_id, "Product 1", "Category 2")
        res = await session.execute(text("SELECT category FROM Products"))
        assert res.scalar_one() == "Category 2"

    @pytest.mark.asyncio
    async def test_insert_customer_success(self, session):
        customer_id = await insert_customer(
            session,
            name="Customer 1",
            email="customer1@example.com",
            phone="1234567890",
        )
        assert isinstance(customer_id, UUID)
        res = await session.execute(text("SELECT count(*) FROM Customers"))
        assert res.scalar() == 1

    @pytest.mark.asyncio
    async def test_update_customer_success(self, session):
        customer_id = await insert_customer(
            session,
            name="Customer 1",
            email="customer1@example.com",
            phone="1234567890",
        )
        await update_customer(
            session,
            customer_id,
            "Customer 1",
            "customer1@example.com",
            "1234567891",
        )
        res = await session.execute(text("SELECT phone FROM Customers"))
        assert res.scalar_one() == "1234567891"

    @pytest.mark.asyncio
    async def test_insert_sale_success(self, session):
        product_id = await insert_product(session, "Product 1", "Category 1")
        customer_id = await insert_customer(
            session,
            name="Customer 1",
            email="customer1@example.com",
            phone="1234567890",
        )
        sale_id = await insert_sale(
            session,
            customer_id=customer_id,
            product_id=product_id,
            sale_date=date.today(),
            amount=100.0,
            quantity=1,
        )
        assert isinstance(sale_id, UUID)
        res = await session.execute(text("SELECT count(*) FROM Sales"))
        assert res.scalar() == 1
