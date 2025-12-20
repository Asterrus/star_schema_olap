from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def load_sales_fact(
    session: AsyncSession,
) -> None:
    """Вставка данных в Sales_Fact"""
    q = """
        INSERT INTO Sales_Fact(
          sale_id,
          product_sk,
          customer_sk,
          sale_date,
          amount,
          quantity
        )
        SELECT
          s.sale_id,
          pd.product_sk,
          cd.customer_sk,
          s.sale_date,
          s.amount,
          s.quantity
        FROM Sales s
        LEFT JOIN Product_Dim pd
          ON pd.product_id = s.product_id
          AND sale_date >= pd.valid_from
          AND (pd.valid_to IS NULL OR sale_date < pd.valid_to)
        LEFT JOIN Customer_Dim cd
          ON cd.customer_id = s.customer_id
          AND sale_date >= cd.valid_from
          AND (cd.valid_to IS NULL OR sale_date < cd.valid_to)
        LEFT JOIN Sales_Fact sf
          ON sf.sale_id = s.sale_id
        WHERE sf.sale_id IS NULL
    """
    await session.execute(text(q))
