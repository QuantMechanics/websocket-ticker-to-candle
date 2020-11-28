CREATE TABLE if not exists stock_symbol(
    exchange text,
    symbol text,
    token text,
    company_name text,
    series text
);

CREATE TABLE if not exists stocks_websocket(
    exchange text,
    price real,
    volume real,
    symbol text,
    ts datetime
);