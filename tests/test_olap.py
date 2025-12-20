from datetime import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from sql_scripts.olap import load_product_dim
from sql_scripts.oltp import insert_product, update_product


class TestLoadProductDim:
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
