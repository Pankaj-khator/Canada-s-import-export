use database canada_import_export;
Use role accountadmin;
use schema public;
use warehouse COMPUTE_WH;

LIST @import_data_from_azure;

create or replace Table HS6_codes_raw(
raw variant
);

copy into HS6_codes_raw 
FROM @import_data_from_azure/HS6_codes.parquet
FILE_FORMAT=(TYPE=PARQUET);

SELECT count(*) from HS6_codes_raw;


create or replace Table country_and_continent_raw(
raw variant
);

copy into country_and_continent_raw 
FROM @import_data_from_azure/country_and_continent.parquet
FILE_FORMAT=(TYPE=PARQUET);

SELECT count(*) from country_and_continent_raw;
