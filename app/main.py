import os
from math import ceil
from clickhouse_driver import Client
from dotenv import load_dotenv
from sqlalchemy.orm import aliased
from sqlalchemy import text, select, func
from fastapi import Depends, FastAPI, Header, HTTPException, Query

from app.utils import process_data_from_database
from app.core.config import settings
from app.core.db import get_async_session, Product, AsyncSession

load_dotenv()

app = FastAPI(
    docs_url="/docs", redoc_url="/redoc",
    title=settings.app_title,
    description=settings.app_description
)

TOKEN = os.getenv('TOKEN')


def check_token(token: str = Header(...)):
    if token != TOKEN:
        raise HTTPException(status_code=403, detail="Отказано в доступе")
    return True


@app.get('/')
async def products_with_column_names(
        page: int = Query(1, gt=0),
        page_size: int = Query(100, gt=0),
        db: AsyncSession = Depends(get_async_session)
):
    offset = (page - 1) * page_size

    # Подсчет общего количества записей
    count_query = select(func.count()).select_from(Product)
    total_count = (await db.execute(count_query)).scalar()

    # Выборка данных с использованием лимита и смещения
    data_query = select(
        Product.classification,
        Product.year,
        Product.period_desc,
        Product.aggregate_level,
        Product.trade_flow_code,
        Product.trade_flow,
        Product.reporter_code,
        Product.reporter,
        Product.partner_code,
        Product.partner,
        Product.commodity_code,
        Product.commodity,
        Product.netweight,
        Product.trade_value
    ).limit(page_size).offset(offset)
    result = await db.execute(data_query)
    data_with_columns = result.fetchall()

    # Преобразование результата в список словарей с именами столбцов
    data_with_columns = [
        {column.name: getattr(row, column.name) for column in Product.__table__.columns}
        for row in data_with_columns
    ]

    total_pages = ceil(total_count / page_size)
    return {
        "total_count": total_count,
        "total_pages": total_pages,
        "current_page": page,
        "data": data_with_columns
    }


@app.post('/api')
async def get_data_from_clickhouse(
        db: AsyncSession = Depends(get_async_session),
        result_type: str = Query(None),
        authorized: bool = Depends(check_token),
):
    # Создаем запрос с использованием SQLAlchemy ORM
    query = select(
        Product.classification,
        Product.year,
        Product.period_desc,
        Product.aggregate_level,
        Product.trade_flow_code,
        Product.trade_flow,
        Product.reporter_code,
        Product.reporter,
        Product.partner_code,
        Product.partner,
        Product.commodity_code,
        Product.commodity,
        Product.netweight,
        Product.trade_value
    ).select_from(Product)

    result = await db.execute(query)
    data_without_id = result.fetchall()  # Извлекаем все строки из результата

    # Получаем имена столбцов из результата, исключая 'id'
    columns = [col for col in result.keys() if col != 'id']
    # Преобразуем результат в список словарей с именами столбцов, исключая 'id'
    data_with_columns = [dict(zip(columns, row)) for row in data_without_id]

    processed_result = await process_data_from_database(data_with_columns)
    if result_type == 'a':
        return {'sum_netweight': processed_result['sum_netweight']}
    elif result_type == 'b':
        return {'partner': processed_result['partner']}
    elif result_type == 'c':
        return {'angola_import': processed_result['angola_import']}
    else:
        return processed_result
