import os
from math import ceil
from clickhouse_driver import Client
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from utils import process_data_from_database

load_dotenv()

app = FastAPI()

TOKEN = os.getenv('TOKEN')

client = Client(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
)


async def check_token(token: str = Header(...)):
    if token != TOKEN:
        raise HTTPException(status_code=403, detail="Отказано в доступе")
    return True


@app.get('/')
async def products_with_column_names(
        page: int = Query(1, gt=0),
        page_size: int = Query(100, gt=0),
):
    offset = (page - 1) * page_size
    query = f"SELECT COUNT(*) FROM product.tables"
    total_count = client.execute(query)[0][0]

    query = f"SELECT * FROM product.tables LIMIT {offset}, {page_size}"
    result = client.execute(query)
    columns = [col[0] for col in client.execute("DESCRIBE product.tables")]
    data_with_columns = [{columns[i]: row[i] for i in range(len(columns))} for row in result]

    total_pages = ceil(total_count / page_size)
    return {
        "total_count": total_count,
        "total_pages": total_pages,
        "current_page": page,
        "data": data_with_columns
    }


@app.post('/api')
async def get_data_from_clickhouse(
        result_type: str = Query(None),
        authorized: bool = Depends(check_token)
):
    query = "SELECT * FROM product.tables"
    result = await client.execute(query)
    columns = [col[0] for col in client.execute("DESCRIBE product.tables")]
    data_with_columns = [{columns[i]: row[i] for i in range(len(columns))} for row in result]
    processed_result = await process_data_from_database(data_with_columns)
    if result_type == 'a':
        return {'sum_netweight': processed_result['sum_netweight']}
    elif result_type == 'b':
        return {'partner': processed_result['partner']}
    elif result_type == 'c':
        return {'angola_import': processed_result['angola_import']}
    else:
        return processed_result
