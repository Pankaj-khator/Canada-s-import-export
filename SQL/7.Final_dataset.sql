use database canada_import_export;
Use role accountadmin;
use schema public;
use warehouse COMPUTE_WH;


SELECT count(*) FROM total_trade;
--- 99134849 ROWS


-- CHEACKING DUPLICATE VALUES FOR HS6_CODE 
SELECT HS6_CODE, COUNT(*)
FROM hs6_codes
GROUP BY HS6_CODE
HAVING COUNT(*) > 1;

select COUNT(*) from hs6_codes;
-- 8644 ROWS

--- REMOVING DUPLICATES

CREATE OR REPLACE TABLE hs6_codes_new as
select *, 
ROW_NUMBER() OVER( PARTITION BY HS6_CODE ORDER BY HS6_CODE ASC) AS rn
FROM hs6_codes;

select * from hs6_codes_new
LIMIT 100;

select count(*) from hs6_codes_new;
-- 6906 TOTAL COUNT 

--- CREATING TABLE WITH NO DUPLICATE
CREATE OR REPLACE TABLE hs6_codes_new1 as
SELECT * from hs6_codes_new
where rn =1;  --- FILTERING VALUES 

select count(*) from hs6_codes_new1;


---- Creating full data table with all the required details

CREATE OR REPLACE TABLE FULL_TOTAL_TRADE_DATA AS
SELECT
    T.*,
    H.*,
    C.*
FROM total_trade AS T
LEFT JOIN hs6_codes_new1 AS H ON T.HS6 = H.HS6_CODE
LEFT JOIN COUNTRY_AND_CONTINENT AS C ON T.COUNTRYCODE = C.CODE;


SELECT COUNT(*) FROM FULL_TOTAL_TRADE_DATA;
--- 99134849 ROWS 

--- Deleting these colomuns because these rae REPEATING OR no longer needed
ALTER TABLE FULL_TOTAL_TRADE_DATA
DROP COLUMN CODE,
HS6,
RN,
HS6_CHAPTERS;


SELECT * FROM FULL_TOTAL_TRADE_DATA
LIMIT 4;

