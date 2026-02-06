CREATE SCHEMA IF NOT EXISTS retail;

DROP TABLE IF EXISTS retail.online_retail;

CREATE TABLE retail.online_retail (
  InvoiceNo TEXT,
  StockCode TEXT,
  Description TEXT,
  Quantity INTEGER,
  InvoiceDate TIMESTAMP,
  UnitPrice NUMERIC,
  CustomerID TEXT,
  Country TEXT
);

COPY retail.online_retail
FROM '/data/source/OnlineRetail.csv'
WITH (FORMAT csv, HEADER true);
