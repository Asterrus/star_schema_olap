from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def create_temp_customers_table(session: AsyncSession):
    q = """
        DROP TABLE IF EXISTS customers_with_hash;
        CREATE TEMP TABLE customers_with_hash AS
        SELECT
          customer_id,
          name,
          email,
          phone,
          md5(coalesce(name,'') || coalesce(email,'') || coalesce(phone,'')) AS attr_hash
        FROM Customers 
    """
    await session.execute(text(q))


async def update_customer_dim(
    session: AsyncSession,
) -> None:
    q = """
    UPDATE Customer_Dim cd
    SET
      valid_to = CURRENT_DATE,
      is_current = FALSE
    FROM customers_with_hash c
    WHERE
      c.customer_id = cd.customer_id
      AND cd.is_current = True
      AND cd.attr_hash <> c.attr_hash
    """
    await session.execute(text(q))


async def insert_customer_dim(
    session: AsyncSession,
) -> None:
    q = """
    INSERT INTO Customer_Dim (
      customer_id,
      name,
      email,
      phone,
      valid_from,
      valid_to,
      is_current,
      attr_hash
    )
    SELECT
      c.customer_id,
      c.name,
      c.email,
      c.phone,
      CURRENT_TIMESTAMP,
      NULL,
      True,
      c.attr_hash
    FROM customers_with_hash c
    LEFT JOIN Customer_Dim cd
      ON cd.customer_id = c.customer_id
      AND cd.is_current = True
    WHERE
      cd.customer_id is NULL
      OR cd.attr_hash <> c.attr_hash
    """
    await session.execute(text(q))


async def load_customer_dim(
    session: AsyncSession,
) -> None:
    """Обновление и вставка данных в Customer_Dim"""
    await create_temp_customers_table(session)
    await update_customer_dim(session)
    await insert_customer_dim(session)
