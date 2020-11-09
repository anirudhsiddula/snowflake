create or replace file format psvdoubleqslash type ='CSV' field_delimiter = '|' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE = '\\';
create or replace file format csvdoubleqslash type ='CSV' field_delimiter = ',' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE = '\\';
create or replace file format psvdoubleqslash_withHead type ='CSV' field_delimiter = '|' FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE = '\\';
create or replace file format csvdoubleqslash_withHead type ='CSV' field_delimiter = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE = '\\';
create or replace file format psvsingleqslash type ='CSV' field_delimiter = '|' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '''' ESCAPE = '\\';
create or replace file format csvsingleqslash type ='CSV' field_delimiter = ',' skip_header=1 FIELD_OPTIONALLY_ENCLOSED_BY = '''' ESCAPE = '\\';
 
