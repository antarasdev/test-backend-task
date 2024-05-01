import asyncio
import json

import pandas as pd


async def process_data_from_database(data):
    if not isinstance(data, pd.DataFrame):
        # Преобразование данных в DataFrame, если это список словарей
        columns = ['classification', 'year', 'period_desc', 'aggregate_level', 'trade_flow_code', 'trade_flow', 'reporter_code', 'reporter',
                   'partner_code', 'partner', 'commodity_code', 'commodity', 'netweight', 'trade_value']
        data = pd.DataFrame(data, columns=columns)

    a = data[(data['commodity_code'].str.startswith('0207')) &
             (data['commodity_code'].str.len() == 6) &
             (data['reporter'] == 'Angola') &
             (data['year'] == 2019)]['netweight'].sum()

    b = data[(data['commodity_code'] == '0207') &
             (data['reporter'] == 'Angola') &
             (data['trade_flow_code'] == 1) &
             (data['period_desc'] == 'January 2019')].nlargest(1, 'netweight')['partner'].to_json()

    c = data[data['commodity'].str.contains('Poultry') &
             (data['trade_flow'] == 'Import') &
             (data['reporter'] == 'Angola')]
    c = c.groupby('partner')['netweight'].sum()
    c = (c / c.sum() * 100).to_json()

    return {
        'sum_netweight': a.item(),
        'partner': json.loads(b),
        'angola_import': json.loads(c)
    }
