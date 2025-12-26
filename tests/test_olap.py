from datetime import datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from sql_scripts.customer_dim import load_customer_dim
from sql_scripts.oltp import (
    insert_customer,
    insert_product,
    insert_sale,
    update_customer,
    update_product,
)
from sql_scripts.product_dim import load_product_dim
from sql_scripts.sales_fact import load_sales_fact


class TestLoadDim:
    """Тесты загрузки данных в таблицы измерений"""

    @pytest.mark.asyncio
    async def test_load_product_dim_success(self, session: AsyncSession):
        """Загрузка данных в таблицу Product_Dim"""
        product_id = await insert_product(session, "Product 1", "Category 1")
        await load_product_dim(session)

        result = await session.execute(text("SELECT * FROM Product_Dim"))
        product_dim = result.fetchone()

        assert product_dim
        assert product_dim.product_id == product_id
        assert product_dim.product_name == "Product 1"
        assert product_dim.category == "Category 1"
        assert product_dim.valid_to is None
        assert isinstance(product_dim.valid_from, datetime)
        assert product_dim.is_current

        await update_product(session, product_id, "Product 1", "Category 2")
        await load_product_dim(session)

        count_res = await session.execute(text("SELECT COUNT(*) FROM Product_Dim"))
        assert count_res.scalar() == 2

        result = await session.execute(
            text("SELECT * FROM Product_Dim WHERE is_current = False")
        )
        previous_product_dim = result.fetchone()
        assert previous_product_dim
        assert previous_product_dim.category == "Category 1"

        result = await session.execute(
            text("SELECT * FROM Product_Dim WHERE is_current = True")
        )

        current_product_dim = result.fetchone()
        assert current_product_dim
        assert current_product_dim.category == "Category 2"

        assert current_product_dim.valid_to is None
        assert previous_product_dim.valid_to is not None
        assert current_product_dim.valid_from >= previous_product_dim.valid_to

        await load_product_dim(session)
        count_res = await session.execute(text("SELECT COUNT(*) FROM Product_Dim"))
        assert count_res.scalar() == 2

    @pytest.mark.asyncio
    async def test_load_customer_dim_success(self, session: AsyncSession):
        """Загрузка данных в таблицу Customer_Dim"""
        customer_id = await insert_customer(
            session,
            name="Customer 1",
            email="customer1@example.com",
            phone="1234567890",
        )
        await load_customer_dim(session)

        result = await session.execute(text("SELECT * FROM Customer_Dim"))
        customer_dim = result.fetchone()
        assert customer_dim
        assert customer_dim.customer_id == customer_id
        assert customer_dim.phone == "1234567890"
        assert customer_dim.valid_to is None
        assert isinstance(customer_dim.valid_from, datetime)
        assert customer_dim.is_current

        await update_customer(
            session,
            customer_id,
            name="Customer 1",
            email="customer1@example.com",
            phone="1234567891",
        )
        await load_customer_dim(session)

        count_res = await session.execute(text("SELECT COUNT(*) FROM Customer_Dim"))
        assert count_res.scalar() == 2

        result = await session.execute(
            text("SELECT * FROM Customer_Dim WHERE is_current = False")
        )
        previous_customer_dim = result.fetchone()
        assert previous_customer_dim
        assert previous_customer_dim.phone == "1234567890"

        result = await session.execute(
            text("SELECT * FROM Customer_Dim WHERE is_current = True")
        )
        current_customer_dim = result.fetchone()
        assert current_customer_dim
        assert current_customer_dim.phone == "1234567891"

        assert current_customer_dim.valid_to is None
        assert previous_customer_dim.valid_to is not None
        assert current_customer_dim.valid_from >= previous_customer_dim.valid_to

        await load_customer_dim(session)
        count_res = await session.execute(text("SELECT COUNT(*) FROM Customer_Dim"))
        assert count_res.scalar() == 2


async def run_etl(session: AsyncSession):
    """Сбор данных аналитики"""
    await load_product_dim(session)
    await load_customer_dim(session)
    await load_sales_fact(session)


class TestLoadFact:
    """Тесты загрузки данных в таблицу фактов Sales_Fact"""

    @pytest.mark.asyncio
    async def test_load_sales_fact_success(self, session: AsyncSession):
        """Тест корректной записи в таблицу Sales_Fact, проверка что не добавляются дубли"""
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
            sale_date=datetime.now(),
            amount=100.0,
            quantity=1,
        )
        await run_etl(session)

        result = await session.execute(text("SELECT * FROM Sales_Fact"))
        sales_fact = result.fetchone()
        assert sales_fact

        await load_sales_fact(session)
        result = await session.execute(text("SELECT COUNT(*) FROM Sales_Fact"))
        assert result.scalar() == 1

    @pytest.mark.asyncio
    async def test_load_sales_fact_correct_dim_selection(self, session: AsyncSession):
        """Тест выбора правильных записей из таблиц измерений"""
        customer_id = await insert_customer(
            session,
            name="Customer 1",
            email="customer1@example.com",
            phone="1234567890",
        )
        product_id = await insert_product(session, "Product 1", "Category 1")
        # Покупка два дня назад
        first_sale_id = await insert_sale(
            session,
            customer_id=customer_id,
            product_id=product_id,
            sale_date=datetime.now() - timedelta(days=2),
            amount=100.0,
            quantity=1,
        )

        await run_etl(session)

        # Обновляем продукт(категория) и покупателя(номер)
        await update_product(session, product_id, "Product 1", "Category 2")
        await update_customer(
            session, customer_id, "Customer 1", "customer1@example.com", "1234567891"
        )

        # Сбор данных аналитики(тут должны быть созданы дополнительные записи в таблицах измерений)
        await load_customer_dim(session)
        await load_product_dim(session)
        await load_sales_fact(session)

        # Покупка сегодня
        second_sale_id = await insert_sale(
            session,
            customer_id=customer_id,
            product_id=product_id,
            sale_date=datetime.now(),
            amount=100.0,
            quantity=1,
        )

        await run_etl(session)

        first_sale_result = await session.execute(
            text("SELECT * FROM Sales_Fact WHERE sale_id = :sale_id"),
            params={"sale_id": first_sale_id},
        )

        first_sale = first_sale_result.fetchone()
        assert first_sale

        second_sale_result = await session.execute(
            text("SELECT * FROM Sales_Fact WHERE sale_id = :sale_id"),
            params={"sale_id": second_sale_id},
        )

        second_sale = second_sale_result.fetchone()
        assert second_sale

        # Проверяем что у продаж разные суррогатные ключи к таблицам измерений
        assert first_sale.product_sk != second_sale.product_sk
        assert first_sale.customer_sk != second_sale.customer_sk

        # Вносим запись о покупке произошедшей день назад, до обновления таблиц измерений
        third_sale_id = await insert_sale(
            session,
            customer_id=customer_id,
            product_id=product_id,
            sale_date=datetime.now() - timedelta(days=1),
            amount=100.0,
            quantity=1,
        )

        await run_etl(session)

        third_sale_result = await session.execute(
            text("SELECT * FROM Sales_Fact WHERE sale_id = :sale_id"),
            params={"sale_id": third_sale_id},
        )

        third_sale = third_sale_result.fetchone()
        # Должны быть выбраны суррогатные ключи, такие же как при первой покупке
        assert third_sale
        assert third_sale.product_sk == first_sale.product_sk
        assert third_sale.customer_sk == first_sale.customer_sk
