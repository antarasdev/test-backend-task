-- Создание базы данных
CREATE DATABASE IF NOT EXISTS product;

CREATE USER IF NOT EXISTS username IDENTIFIED WITH plaintext_password BY 'password';

GRANT ALL ON product.* TO username;

-- Создание таблицы
CREATE TABLE IF NOT EXISTS product.tables (
    classification String,
    year UInt16,
    period_desc String,
    aggregate_level UInt8,
    trade_flow_code UInt8,
    trade_flow String,
    reporter_code UInt8,
    reporter String,
    partner_code UInt8,
    partner String,
    commodity_code String,
    commodity String,
    netweight UInt32,
    trade_value UInt32
) ENGINE = MergeTree()
ORDER BY (period_desc);

INSERT INTO product.tables (
    classification,
    year,
    period_desc,
    aggregate_level,
    trade_flow_code,
    trade_flow,
    reporter_code,
    reporter,
    partner_code,
    partner,
    commodity_code,
    commodity,
    netweight,
    trade_value)
FROM INFILE '/data/Data.csv' SETTINGS format_csv_delimiter=';' FORMAT CSVWithNames;

