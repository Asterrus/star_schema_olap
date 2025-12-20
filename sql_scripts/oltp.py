from datetime import date
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def insert_product(
    session: AsyncSession, product_name: str, category: str
) -> UUID:
    q = text(
        "INSERT INTO Products (product_name, category) VALUES (:product_name, :category) RETURNING product_id"
    )
    res = await session.execute(
        q,
        {
            "product_name": product_name,
            "category": category,
        },
    )
    product_id = res.scalar_one()

    return product_id


async def update_product(
    session: AsyncSession, product_id: UUID, product_name: str, category: str
):
    q = text("""
        UPDATE Products
        SET 
          product_name =:product_name,
          category =:category
        WHERE product_id =:product_id
    """)
    await session.execute(
        q,
        {"product_name": product_name, "category": category, "product_id": product_id},
    )


async def insert_customer(
    session: AsyncSession, name: str, email: str, phone: str
) -> UUID:
    q = text(
        "INSERT INTO Customers (name, email, phone) VALUES (:name, :email, :phone) RETURNING customer_id"
    )
    res = await session.execute(
        q,
        {
            "name": name,
            "email": email,
            "phone": phone,
        },
    )
    customer_id = res.scalar_one()

    return customer_id


async def insert_sale(
    session: AsyncSession,
    customer_id: UUID,
    product_id: UUID,
    sale_date: date,
    amount: float,
    quantity: int,
) -> UUID:
    q = text(
        """
        INSERT INTO Sales (customer_id, product_id, sale_date, amount, quantity)
        VALUES (:customer_id, :product_id, :sale_date, :amount, :quantity)
        RETURNING sale_id"""
    )

    res = await session.execute(
        q,
        {
            "customer_id": customer_id,
            "product_id": product_id,
            "sale_date": sale_date,
            "amount": amount,
            "quantity": quantity,
        },
    )
    sale_id = res.scalar_one()

    return sale_id
