
CREATE OR REPLACE PROCEDURE DB_NAME.SCHEMA_NAME.PROCEDURE_NAME ("PREFIX" VARCHAR(16777216), "DROP_OBJECTS_WITH_PREFIX" BOOLEAN DEFAULT FALSE, "CREATE_OR_REPLACE_TABLES" BOOLEAN DEFAULT TRUE)
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES
('snowflake-snowpark-python')
HANDLER = 'handler'
EXECUTE AS CALLER
AS '
def handler(session, PREFIX: str, DROP_OBJECTS_WITH_PREFIX: bool = False, CREATE_OR_REPLACE_TABLES: bool =True):

  if DROP_OBJECTS_WITH_PREFIX:
    session.sql("show tables in database DATABASE_NAME").collect()
    
    ddls_to_delete_rows = session.sql(f"""
    select * from (
      select
        $2 as name,
        $3 as database_name,
        $4 as schema_name
      from table(result_scan (last_query_id())))
      where database_name = ''DATABASE_NAME''
      and name ilike ''(PREFIX}%''
      """).collect()
    
    drop_statements_executed: str = ""
    
    for row in ddls_to_delete_rows:
      session.sql(f" drop table {row.DATABASE_NAME}.{row.SCHEMA_NAME}. {row.NAME}").collect()
      drop_statements_executed += f"drop table {row.DATABASE_NAME}.{row.SCHEMA_NAME}. {row.NAME}" + ";\\n"
    
    if not CREATE_OR_REPLACE_TABLES:
      return drop_statements_executed
  
  
  session.sql("select get_ddl(''DATABASE'', ''DATABASE_NAME'', true)").collect()
  extracted_ddls: str = ""
    
  parsed_ddls_rows: list = session.sql(f"""
    select replace(replace(replace(value, ''.SOURCE_SCHEMA.'', ''.TARGET_SCHEMA.'' || ''{PREFIX}'' ||''_''),
    ''.SOURCE_SCHEMA_2.'',
    ''.TARGET_SCHEMA_2.'' || ''{PREFIX}'' || ''_''),
    ''.SOURCE_SCHEMA_3.'', ''.TARGET_SCHEMA_3.''|| ''{PREFIX}'' || ''_'') as parsed_ddls
    from
      (
        select $1 as ddl_statements_string
        from table(result_scan(last_query_id()))
      ) statements_from_get_ddl,
    lateral split_to_table(statements_from_get_ddl.ddl_statements_string, '';'')
    where 1=1
      --exclude not needed schemas
      and value not ilike ''%.PUBLIC.%''
      --exclude database and schema ddl
      and value not ilike ''%create or replace database%''
      and value not ilike ''%create or replace schema%''
      --include only DDL statements
      and value ilike ''%create or replace%''
    """).collect()
  
  for ddl in parsed_ddls_rows:
    extracted_ddls += ddl.parsed_ddls + "; \\n"
  
  if CREATE_OR_REPLACE_TABLES:
    for ddl in parsed_ddls_rows:
    session.sql(ddl.parsed_ddls).collect()
    
  return extracted_ddls
  ';
