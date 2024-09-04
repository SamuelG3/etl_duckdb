import os
import gdown
import duckdb
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

url_pasta = 'https://drive.google.com/drive/folders/15ysKK_wVVYonp_pxomYrnySzPKOMAqak?usp=sharing'
diretorio_local = './pasta_gdown'


def baixar_arquivos_gdrive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False)

def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_arquivos = os.listdir(diretorio)

    for arquivo in todos_arquivos:
        if arquivo.endswith('.csv'):
            caminho_completo = os.path.join()
            arquivos_csv.append(caminho_completo)

    return arquivos_csv

def read_csv(caminho_arquivo):
    dataframe_duckdb = duckdb.read_csv(caminho_arquivo)
    return dataframe_duckdb

def transformar(df: duckdb.DuckDBPyRelation) -> duckdb.DataFrame:
    try:
        # Execute the SQL query
        df_transformado = duckdb.sql("""
            SELECT *
            FROM df
            """)
        
        # Add the new column
        df_transformado = df_transformado.with_column(
            duckdb.sql.F.expr("quantidade * valor AS total_vendas")
        )
        
        # Convert to DataFrame
        df_transformado = df_transformado.df()
        
        return df_transformado
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def salvar_postgres(df_duckdb, table):
    load_dotenv()

    DATABASE_URL = os.getenv("DATABASE_URL") # EX: 'postgresql://user:password@localhost:5432/database"
    engine = create_engine(DATABASE_URL)
    df_duckdb.to_sql(table, con=engine, if_exists='append', index=False)

if __name__ == '__main__':
    url_pasta = 'https://drive.google.com/drive/folders/15ysKK_wVVYonp_pxomYrnySzPKOMAqak?usp=sharing'
    diretorio_local = './pasta_gdown'
    
    baixar_arquivos_gdrive(url_pasta, diretorio_local)