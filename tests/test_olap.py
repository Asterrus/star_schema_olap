from datetime import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from sql_scripts.customer_dim import load_customer_dim
from sql_scripts.oltp import (
    insert_customer,
    insert_product,
    update_customer,
    update_product,
)
from sql_scripts.product_dim import load_product_dim


class TestLoadDim:
    @pytest.mark.asyncio
    async def test_load_product_dim_success(self, session: AsyncSession):
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
