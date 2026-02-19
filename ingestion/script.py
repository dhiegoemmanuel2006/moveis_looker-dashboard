import kagglehub
import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

print("Starting script execution...")

#  1. Load environment variables from .env file
load_dotenv()

# 1. Monitorar o Download
print("Baixando dataset do Kaggle...")
path = kagglehub.dataset_download("saketsingh9728/movies-dataset")
print(f"Dataset baixado em: {path}")

# 2. Validating the Dataset and Load into DataFrame
file_path = os.path.join(path, "movies_dataset.csv")
if os.path.exists(file_path):
    df = pd.read_csv(file_path)
    print(f"Dataset carregado. Linhas: {len(df)} | Colunas: {len(df.columns)}")
else:
    print(f"Erro: Arquivo {file_path} não encontrado.")
    exit()


# 3. Connecting to the Database
print(f"Conectando ao host: {os.getenv('DB_HOST')}...")
try:
    engine = create_engine(f'postgresql://{os.getenv("DB_USER")}:{os.getenv("DB_PASSWORD")}@{os.getenv("DB_HOST")}:{os.getenv("DB_PORT")}/{os.getenv("DB_NAME")}')
    # Testing the connection
    with engine.connect() as conn:
        print("Conexão estabelecida com sucesso.")
except Exception as e:
    print(f"Falha na conexão: {e}")
    exit()

# 4. Creating the Bronze Table
print("Criando tabela 'bronze.movies'e schema bronze, silver e gold...")
with engine.begin() as conn:
    try:
        with open('bronze/movies-csv/CREATE_TABLE_BRONZE_MOVIES.sql', 'r') as file:
            create_table_query = file.read()
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold;"))
        conn.execute(text(create_table_query))
        print("Tabela 'bronze.movies' criada ou verificada com sucesso. Schemas Bronze, Silver and Gold criado com sucesso.")
    except Exception as e:
        print(f"Erro ao criar tabela: {e}")
        exit()


# 5. Inserting Data into the Bronze Table
print("Carregando dados para a tabela 'bronze.movies'...")
print(f"Dados a serem inseridos: {len(df)} linhas.")
df.to_sql('movies', schema='bronze', con=engine, if_exists='append', index=False)
print("Carga finalizada com sucesso.")


# 6. Creating the Silver tablea
print("Criando tabela 'silver.movies'...")
with engine.begin() as conn:
    try:
        with open('silver/movies/CREATE_TABLE_SILVER_MOVIES.sql', 'r') as file:
            create_table_query = file.read()
        conn.execute(text(create_table_query))
        print("Tabela 'silver.movies' criada ou verificada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar tabela: {e}")
        exit()

print("Creating the Gold view...")
with engine.connect() as conn:
    try:
        with open('gold/movies/CREATE_VIEW_GOLD_MOVIES.sql', 'r') as file:
            create_view_query = file.read()
        conn.execute(text(create_view_query))
        print("View 'gold.movies' criada ou verificada com sucesso.")
    except Exception as e:
        print(f"Erro ao criar view: {e}")
        exit()
print("--- Script finalizado ---")