use role ACCOUNTADMIN;
create or replace role Data_analyst;
use warehouse COMPUTE_WH;

create or replace Database canada_import_export;
create database canada_import_export;
Create or replace schema analyst;