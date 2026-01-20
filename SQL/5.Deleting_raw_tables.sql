use role accountadmin;
USE DATABASE canada_import_export;
use warehouse COMPUTE_WH;
use schema PUBLIC;

---   import_data_from_azure is the stage for Azure


Create or replace PROCEDURE Delete_all_import_row_files()
RETURNS STRING
LANGUAGE SQL
EXECUTE as caller
AS
$$
Declare
SQL_QUERY STRING; 
yr INTEGER;          -- Years

BEGIN
FOR yr IN ARRAY_CONSTRUCT(2002,2003,2004,2005,2006,2007,2008,2009,2010,
                            2011,2012,2013,2014,2015,2016,2017,2018,2019,
                              2020,2021,2022) DO

SQL_QUERY:='DROP TABLE IF EXISTS IMPORT_' || yr || '_RAW';
EXECUTE IMMEDIATE SQL_QUERY;
END FOR;

RETURN ' Tables Deleted ';
END;
$$;

call Delete_all_import_row_files();

--------------------------------------------------------------------------------------------------

-- Deleting all Export Raw files 
Create or replace PROCEDURE Delete_all_export_row_files()
RETURNS STRING
LANGUAGE SQL
EXECUTE as caller
AS
$$
Declare
SQL_QUERY STRING;
yr INTEGER;
BEGIN
FOR yr IN ARRAY_CONSTRUCT(2002,2003,2004,2005,2006,2007,2008,2009,2010,
                            2011,2012,2013,2014,2015,2016,2017,2018,2019,
                              2020,2021,2022) DO

SQL_QUERY:='DROP TABLE IF EXISTS EXPORT_' || yr || '_RAW';
EXECUTE IMMEDIATE SQL_QUERY;

END FOR;
RETURN ' Tables Deleted ';
END;
$$;

call Delete_all_export_row_files();
