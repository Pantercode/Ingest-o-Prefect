from prefect import task, Flow
import pyodbc
import pandas as pd

server = "NDUXSP0014"
database = "ingestion"

def get_sql_server_connection():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )

@task
def listar_tabelas():
    conn = get_sql_server_connection()
    query_tabelas = """
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_TYPE = 'BASE TABLE'
    """
    df_tabelas = pd.read_sql(query_tabelas, conn)
    conn.close()
    return df_tabelas

@task
def processar_tabela(tabela_escolhida):
    conn = get_sql_server_connection()
    query = f"SELECT * FROM {tabela_escolhida}"
    df = pd.read_sql(query, conn)
    conn.close()
    
    df = df.copy()
    df.drop_duplicates(inplace=True)
    
    if 'month' in df.columns:
        df['months'] = pd.to_datetime(df['month'], format='%Y-%m')
    
    colunas_inteiro = [col for col in ['hot_tub', 'pool'] if col in df.columns]
    if colunas_inteiro:
        df[colunas_inteiro] = df[colunas_inteiro].astype(int)
    
    print(f"✅ Processamento da tabela {tabela_escolhida} concluído!")
    return df

with Flow("etl_sql_server") as flow:
    tabelas = listar_tabelas()
    df_amenities = processar_tabela("airbn.amenities")
    df_geolocation = processar_tabela("airbn.geolocation")
    df_market_analysis = processar_tabela("airbn.market_analysis")

if __name__ == "__main__":
    flow.run()
