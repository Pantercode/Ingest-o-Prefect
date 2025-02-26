
from prefect import task, Flow
import kagglehub
import shutil
import os

destino = r"C:\Users\marcell.oliveira\Desktop\airbn\Ingestao"

@task
def instalar_kagglehub():
    os.system("pip install kagglehub")

@task
def baixar_e_mover_dataset():
    os.makedirs(destino, exist_ok=True)
    
    # Baixar dataset
    path = kagglehub.dataset_download("computingvictor/zillow-market-analysis-and-real-estate-sales-data")
    
    # Mover os arquivos para o destino
    for arquivo in os.listdir(path):
        shutil.move(os.path.join(path, arquivo), os.path.join(destino, arquivo))
    
    print(f"Arquivos movidos para: {destino}")

with Flow("ingestao_kaggle_airbnb") as flow:
    instalar_kagglehub()
    baixar_e_mover_dataset()

if __name__ == "__main__":
    flow.run()