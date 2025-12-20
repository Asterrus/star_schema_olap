from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def create_temp_products_table(session: AsyncSession):
    q = """
        DROP TABLE IF EXISTS products_with_hash;
        CREATE TEMP TABLE products_with_hash AS
        SELECT
          product_id,
          product_name,
          category,
          md5(coalesce(product_name,'') || coalesce(category,'')) AS attr_hash
        FROM Products 
    """
    await session.execute(text(q))


async def update_product_dim(
    session: AsyncSession,
) -> None:
    q = """
    UPDATE Product_Dim pd
    SET
      valid_to = CURRENT_DATE,
      is_current = FALSE
    FROM products_with_hash p
    WHERE
      p.product_id = pd.product_id
      AND pd.is_current = True
      AND pd.attr_hash <> p.attr_hash
    """
    await session.execute(text(q))


async def insert_product_dim(
    session: AsyncSession,
) -> None:
    q = """
    INSERT INTO Product_Dim (
      product_id,
      product_name,
      category,
      valid_from,
      valid_to,
      is_current,
      attr_hash
    )
    SELECT
      p.product_id,
      p.product_name,
      p.category,
      CURRENT_TIMESTAMP,
      NULL,
      True,
      p.attr_hash
    FROM products_with_hash p
    LEFT JOIN Product_Dim pd
      ON pd.product_id = p.product_id
      AND pd.is_current = True
    WHERE
      pd.product_id is NULL
      OR pd.attr_hash <> p.attr_hash
    """
    await session.execute(text(q))


async def load_product_dim(
    session: AsyncSession,
) -> None:
    """Обновление и вставка данных в Product_Dim"""
    await create_temp_products_table(session)
    await update_product_dim(session)
    await insert_product_dim(session)
