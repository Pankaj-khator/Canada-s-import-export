use role accountadmin;
use warehouse COMPUTE_WH;

create or replace stage import_data_from_azure
URL='Azure://pankajdatalake.blob.core.windows.net/canada-import-export'
credentials =(AZURE_SAS_TOKEN='sp=racwdlmeop&st=2026-01-15T23:32:22Z&se=2026-11-30T07:47:22Z&spr=https&sv=2024-11-04&sr=c&sig=sDgaQ6W%2F1%2FNEb5FcF3YuZsW8qrIMtbdrYjJGQj6beig%3D')
File_format=(Type=PARQUET);

-- For Import Data

CREATE OR REPLACE PROCEDURE LoadData_From_Azure()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    yr INTEGER;
    SQL_Query STRING;
BEGIN
    -- loop over array of years to avoid WHILE parsing issues
    FOR yr IN ARRAY_CONSTRUCT(2002,2003,2004,2005,2006,2007,2008,2009,2010,
                              2011,2012,2013,2014,2015,2016,2017,2018,2019,
                              2020,2021,2022) DO

        -- 1. Create RAW table
        SQL_Query := 'CREATE OR REPLACE TABLE Import_' || yr || '_raw (raw VARIANT);';
        EXECUTE IMMEDIATE SQL_Query;

        -- 2. Copy Parquet into RAW table
        SQL_Query := 'COPY INTO Import_' || yr || '_raw FROM @import_data_from_azure/Import_' || yr || '.parquet FILE_FORMAT=(TYPE=PARQUET);';
        EXECUTE IMMEDIATE SQL_Query;

        -- 3. Create typed table
        SQL_Query := 'CREATE OR REPLACE TABLE Import_' || yr || ' AS SELECT raw:Year::BIGINT AS Year, raw:CountryCode::VARCHAR(25) AS CountryCode, raw:HS6::BIGINT AS HS6, raw:Province::VARCHAR(25) AS Province, raw:Quantity::BIGINT AS Quantity, raw:Value::NUMBER(18,2) AS Value, raw:MeasurementUnit::VARCHAR(25) AS MeasurementUnit FROM Import_' || yr || '_raw;';
        EXECUTE IMMEDIATE SQL_Query;

    END FOR;

    RETURN 'Import data loaded successfully';
END;
$$;


CALL LoadData_From_Azure();


-- For Export Data

CREATE OR REPLACE PROCEDURE LoadData_Export_From_Azure()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    yr INTEGER;
    SQL_Query STRING;
BEGIN
    -- loop over array of years to avoid WHILE parsing issues
    FOR yr IN ARRAY_CONSTRUCT(2002,2003,2004,2005,2006,2007,2008,2009,2010,
                              2011,2012,2013,2014,2015,2016,2017,2018,2019,
                              2020,2021,2022) DO

        -- 1. Create RAW table
        SQL_Query := 'CREATE OR REPLACE TABLE Export_' || yr || '_raw (raw VARIANT);';
        EXECUTE IMMEDIATE SQL_Query;

        -- 2. Copy Parquet into RAW table
        SQL_Query := 'COPY INTO Export_' || yr || '_raw FROM @import_data_from_azure/Export_' || yr || '.parquet FILE_FORMAT=(TYPE=PARQUET);';
        EXECUTE IMMEDIATE SQL_Query;

        -- 3. Create typed table
        SQL_Query := 'CREATE OR REPLACE TABLE Export_' || yr || ' AS SELECT 
        raw:Year::BIGINT AS Year, 
        raw:CountryCode::VARCHAR(25) AS CountryCode,
        raw:HS6::BIGINT AS HS6, 
        raw:Province::VARCHAR(25) AS Province,
        raw:Quantity::BIGINT AS Quantity,
        raw:Value::NUMBER(18,2) AS Value,
        raw:MeasurementUnit::VARCHAR(25) AS MeasurementUnit
        FROM Export_' || yr || '_raw;';
        EXECUTE IMMEDIATE SQL_Query;

    END FOR;

    RETURN 'Export data loaded successfully';
END;
$$;


CALL LoadData_Export_From_Azure();


