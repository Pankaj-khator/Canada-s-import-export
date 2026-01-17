
use database canada_import_export;

--- testing on one table before looping on all the tables
select * from import_2002
limit 2;

-- Changine columns name year to YYYYMM 
ALTER TABLE Import_2002
RENAME COLUMN year TO YYYYMM;

-- testing the logic
SELECT
    year AS year_month,
    FLOOR(year / 100) AS year_only,
    MOD(year, 100) AS month,
    FLOOR(HS6/10000) as hs6_chapter
FROM import_2002
limit 10;

-- Adding required column on one table
ALTER TABLE import_2002
ADD COLUMN year_only INT,
   month INT,
   hs6_chapter INT,
   Trade varchar(24);

-- Filling values in the columns
UPDATE Import_2002
SET
year_only = FLOOR(YYYYMM/100),
month=MOD(YYYYMM, 100),
hs6_chapter = FLOOR(HS6/10000),
Trade = 'Import';

-- testing final result on the table
Select * from import_2002
limit 2000;


-- adding columns of months, YEAR_ONLY from YYYYMM column and HS6_CHAPTER from HS6 columsn
-- HS6 chapter are from 1-99
-- so if we remove last 4 digit of hs6 we will get hs6 chapter


-- create or replace procedure adding_columns()

-- PROCEDURE for IMPORT Data
CREATE or replace PROCEDURE Data_prepare_import()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
SQL_Query STRING;
yr INT;
BEGIN
FOR yr IN ARRAY_CONSTRUCT(2002,2003,2004,2005,2006,2007,2008,2009,2010,
                              2011,2012,2013,2014,2015,2016,2017,2018,2019,
                              2020,2021,2022) DO
                              
SQL_Query:= '
ALTER TABLE Import_'|| yr ||'
RENAME COLUMN year TO YYYYMM;';
EXECUTE IMMEDIATE SQL_Query;

SQL_Query:= 'ALTER TABLE Import_'|| yr ||'
ADD COLUMN year_only INT,
month INT,
hs6_chapter INT,
Trade varchar(24);';

EXECUTE IMMEDIATE SQL_Query;

SQL_Query:='UPDATE Import_'|| yr ||'
SET
year_only = FLOOR(YYYYMM/100),
month=MOD(YYYYMM, 100),
hs6_chapter = FLOOR(HS6/10000),
Trade = ''Import'';';

EXECUTE IMMEDIATE SQL_Query;

END FOR;
RETURN 'DONE';
END;
$$;


call Data_prepare_import();


-- PROCEDURE for Export Data
CREATE or replace PROCEDURE Data_prepare_export()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
SQL_Query STRING;
yr INT;
BEGIN
FOR yr IN ARRAY_CONSTRUCT(2002,2003,2004,2005,2006,2007,2008,2009,2010,
                              2011,2012,2013,2014,2015,2016,2017,2018,2019,
                              2020,2021,2022) DO
                              
SQL_Query:= '
ALTER TABLE Export_'|| yr ||'
RENAME COLUMN year TO YYYYMM;';
EXECUTE IMMEDIATE SQL_Query;

SQL_Query:= 'ALTER TABLE Export_'|| yr ||'
ADD COLUMN year_only INT,
month INT,
hs6_chapter INT,
Trade varchar(24);';

EXECUTE IMMEDIATE SQL_Query;

SQL_Query:='UPDATE Export_'|| yr ||'
SET
year_only = FLOOR(YYYYMM/100),
month=MOD(YYYYMM, 100),
hs6_chapter = FLOOR(HS6/10000),
Trade = ''Export'';';

EXECUTE IMMEDIATE SQL_Query;

END FOR;
RETURN 'DONE';
END;
$$;


call Data_prepare_export();

-- Creating a table of total imports

CREATE OR REPLACE VIEW total_imports AS
SELECT * FROM import_2002
UNION ALL SELECT * FROM import_2003
UNION ALL SELECT * FROM import_2004
UNION ALL SELECT * FROM import_2005
UNION ALL SELECT * FROM import_2006
UNION ALL SELECT * FROM import_2007
UNION ALL SELECT * FROM import_2008
UNION ALL SELECT * FROM import_2009
UNION ALL SELECT * FROM import_2010
UNION ALL SELECT * FROM import_2011
UNION ALL SELECT * FROM import_2012
UNION ALL SELECT * FROM import_2013
UNION ALL SELECT * FROM import_2014
UNION ALL SELECT * FROM import_2015
UNION ALL SELECT * FROM import_2016
UNION ALL SELECT * FROM import_2017
UNION ALL SELECT * FROM import_2018
UNION ALL SELECT * FROM import_2019
UNION ALL SELECT * FROM import_2020
UNION ALL SELECT * FROM import_2021
UNION ALL SELECT * FROM import_2022;




-- Creating a table of total imports

CREATE OR REPLACE VIEW total_exports AS
SELECT * FROM import_2002
UNION ALL SELECT * FROM export_2003
UNION ALL SELECT * FROM export_2004
UNION ALL SELECT * FROM export_2005
UNION ALL SELECT * FROM export_2006
UNION ALL SELECT * FROM export_2007
UNION ALL SELECT * FROM export_2008
UNION ALL SELECT * FROM export_2009
UNION ALL SELECT * FROM export_2010
UNION ALL SELECT * FROM export_2011
UNION ALL SELECT * FROM export_2012
UNION ALL SELECT * FROM export_2013
UNION ALL SELECT * FROM export_2014
UNION ALL SELECT * FROM export_2015
UNION ALL SELECT * FROM export_2016
UNION ALL SELECT * FROM export_2017
UNION ALL SELECT * FROM export_2018
UNION ALL SELECT * FROM export_2019
UNION ALL SELECT * FROM export_2020
UNION ALL SELECT * FROM export_2021
UNION ALL SELECT * FROM export_2022;


select sum(value) from total_exports
where countrycode = 'US';

select sum(value) from total_exports
where;