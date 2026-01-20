use database canada_import_export;
Use role accountadmin;
use schema public;
use warehouse COMPUTE_WH;
----------------------------------------------------------------
-- PREPARING COUNTRY AND CONTINENT TABLE


SELECT * FROM COUNTRY_AND_CONTINENT_RAW
LIMIT 1;

CREATE OR REPLACE TABLE COUNTRY_AND_CONTINENT AS
SELECT
raw:country::varchar(24) as country,
raw:code::varchar(24) as code,
raw:continent::varchar(24) as continent
from COUNTRY_AND_CONTINENT_RAW;

SELECT * FROM COUNTRY_AND_CONTINENT
LIMIT 1;


-----------------------------------------------------------
-- PREPARING HS6 TABLE

SELECT * FROM hs6_codes_raw
LIMIT 1;


CREATE OR REPLACE TABLE hs6_codes AS
SELECT
raw:HS6_Code::INT as HS6_Code,
raw:chapters::INT as chapters,
raw:chapterName::varchar(24) as chapterName,
raw:Product::varchar(24) as Product
from hs6_codes_raw;


SELECT * FROM hs6_codes;