\c itv003722_retail_db;

DROP TABLE IF EXISTS Stockcompanies CASCADE;
DROP TABLE IF EXISTS StockPrices CASCADE;

CREATE TABLE Stockcompanies(symbol varchar NOT NULL,
	Security varchar NOT NULL,
	Sector varchar NOT NULL,
	Sub_Industry varchar NOT NULL,
	Headquarter varchar NOT NULL

);


\copy  Stockcompanies(symbol,Security,Sector,Sub_Industry,Headquarter) FROM '/home/itv003722/E2E/Stockcompanies.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE StockPrices(
	date varchar NOT NULL,
	symbol varchar,
	open double precision,
	close double precision,
	low double precision,
	high double precision,
	volume int

);

\copy  StockPrices(date,symbol,open,close,low ,high,volume) FROM '/home/itv003722/E2E/StockPrices.csv' DELIMITER ',' CSV HEADER;

UPDATE StockPrices SET date=to_date(date, 'MM/DD/YYYY');

select * from Stockcompanies limit 10;
select * from StockPrices limit 10;
