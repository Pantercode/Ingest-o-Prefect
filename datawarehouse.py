from prefect import task, Flow
import pyodbc
import pandas as pd
import os
from sqlalchemy import create_engine, text
from snowflake.sqlalchemy import URL

server = "NDUXSP0014"
database = "ingestion"
sf_user = "marcell1989"
sf_password = "Am@ndl@2025"
sf_account = "ravtzvp-nk53799"

def get_sql_server_connection():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )

def get_snowflake_engine():
    sf_url = URL(
        account=sf_account,
        user=sf_user,
        password=sf_password,
        database="DW_AIRBN_Gold",
        schema="PUBLIC",
        warehouse="DW_AIRBN_Gold"
    )
    return create_engine(sf_url)

@task
def obter_tabelas_sql_server():
    conn = get_sql_server_connection()
    query_tables = """
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    """
    tables_df = pd.read_sql(query_tables, conn)
    conn.close()
    return [f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}" for _, row in tables_df.iterrows()]

@task
def transferir_dados_para_snowflake(tables):
    conn_sql = get_sql_server_connection()
    sf_engine = get_snowflake_engine()
    
    for table in tables:
        schema, table_name = table.split(".")
        query_schema = f"""
            SELECT COLUMN_NAME, DATA_TYPE 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{schema}'
        """
        schema_df = pd.read_sql(query_schema, conn_sql)
        columns_sql = ", ".join([f'"{row.COLUMN_NAME}" {row.DATA_TYPE.upper()}' for _, row in schema_df.iterrows()])
        create_table_sql = f'CREATE TABLE IF NOT EXISTS DW_AIRBN_Gold.PUBLIC."{table_name}" ({columns_sql})'
        
        with sf_engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        for chunk in pd.read_sql(f'SELECT * FROM [{schema}].[{table_name}]', conn_sql, chunksize=100):
            chunk.to_sql(table_name, con=sf_engine, index=False, if_exists='append', method='multi')
            print(f"✅ {len(chunk)} linhas enviadas para {table_name}...")
        
        print(f"✅ Tabela {table_name} carregada no Snowflake!")
    
    conn_sql.close()

with Flow("migracao_sql_server_para_snowflake") as flow:
    tables = obter_tabelas_sql_server()
    transferir_dados_para_snowflake(tables)

if __name__ == "__main__":
    flow.run()
