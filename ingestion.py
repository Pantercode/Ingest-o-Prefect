from prefect import task, Flow
import os
import pandas as pd
import pyodbc

server = "NDUXSP0014"  
database = "master"  
conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};trusted_connection=yes"

diretorio_csv = r"C:\Users\marcell.oliveira\Desktop\airbn\Ingestao"

@task
def criar_banco_e_schema():
    conn = pyodbc.connect(conn_str, autocommit=True)
    cursor = conn.cursor()
    cursor.execute("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'ingestion') CREATE DATABASE ingestion")
    cursor.close()
    conn.close()
    
    novo_conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE=ingestion;trusted_connection=yes"
    conn = pyodbc.connect(novo_conn_str)
    cursor = conn.cursor()
    cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'airbn') EXEC('CREATE SCHEMA airbn')")
    conn.commit()
    cursor.close()
    conn.close()

@task
def processar_csv():
    novo_conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE=ingestion;trusted_connection=yes"
    conn = pyodbc.connect(novo_conn_str)
    cursor = conn.cursor()
    
    arquivos_csv = [f for f in os.listdir(diretorio_csv) if f.endswith('.csv')]
    
    for arquivo in arquivos_csv:
        caminho_arquivo = os.path.join(diretorio_csv, arquivo)
        nome_tabela = os.path.splitext(arquivo)[0]
        nome_completo_tabela = f"airbn.{nome_tabela}"

        try:
            chunk_iter = pd.read_csv(caminho_arquivo, delimiter=";", chunksize=100)
            first_chunk = next(chunk_iter)
            colunas = ", ".join([f"[{col}] VARCHAR(MAX)" for col in first_chunk.columns])
            
            sql_create = f"""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'airbn' AND TABLE_NAME = '{nome_tabela}')
            CREATE TABLE {nome_completo_tabela} ({colunas})
            """
            cursor.execute(sql_create)
            conn.commit()
            
            for chunk in pd.read_csv(caminho_arquivo, delimiter=";", chunksize=100):
                valores = [tuple(row.fillna('NULL')) for _, row in chunk.iterrows()]
                colunas_insert = ", ".join([f"[{col}]" for col in chunk.columns])
                placeholders = ", ".join(["?" for _ in chunk.columns])
                sql_insert = f"INSERT INTO {nome_completo_tabela} ({colunas_insert}) VALUES ({placeholders})"
                cursor.executemany(sql_insert, valores)
                conn.commit()
            
            print(f"Tabela {nome_completo_tabela} criada e dados inseridos com sucesso!")
        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")
    
    cursor.close()
    conn.close()

with Flow("ingestao_sql_server") as flow:
    criar_banco_e_schema()
    processar_csv()

if __name__ == "__main__":
    flow.run()
