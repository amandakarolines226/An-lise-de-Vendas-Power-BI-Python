"""analise_de_vendas_com_power_bi_python.py"""

import time
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class ExcelChangeHandler(FileSystemEventHandler):
    """
    Manipulador de eventos para detectar alterações em arquivos CSV.

    Quando um arquivo CSV for modificado, essa classe recarrega os dados
    e executa o processamento automaticamente.
    """
    def on_modified(self, event):
        if event.src_path.endswith(".csv"):
            print("Arquivo modificado")
            # Recarrega os dados e executa o processamento
            dados = carregar_dados(event.src_path)
            converter_colunas(dados)

# Função que carrega os dados do CSV alterado
def carregar_dados(caminho_arquivo):
    """
    Carrega os dados de um arquivo CSV para um DataFrame.

    Parâmetros:
    caminho_arquivo (str): Caminho completo do arquivo CSV.

    Retorna:
    pandas.DataFrame: Dados carregados e formatados.
    """
    dados = pd.read_csv(caminho_arquivo, sep=';', skipinitialspace=True)
    dados.columns = dados.columns.str.strip()  # remove espaços nas colunas
    print("Primeiras linhas do CSV:")
    print(dados.head())
    return dados

# Converter colunas
def converter_colunas(dados):
    """
    Converte colunas específicas do DataFrame:
    - Converte a coluna 'Data da Venda' para datetime.
    - Remove símbolos monetários e converte 'Preço Unitario' para float.
    - Calcula a coluna 'Receita Total'.

    Parâmetros:
    dados (pandas.DataFrame): Dados a serem transformados.
    """
    dados["Data da Venda"] = pd.to_datetime(dados["Data da Venda"], dayfirst=True)
    dados["Preco Unitario"] = (
        dados["Preco Unitario"]
        .replace(r"R\$", "", regex=True)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )
    dados["Receita Total"] = (
        dados["Quantidade Vendida"].astype(int) * dados["Preco Unitario"]
    )

    # Verificar nulos e remover se necessário
    dados.dropna(inplace=True)

    # Calcular receita caso esteja faltando
    if "Receita Calculada" not in dados.columns:
        dados["Receita Calculada"] = dados["Quantidade Vendida"] * dados["Preco Unitario"]

    # Salvar novo arquivo limpo
    dados.to_csv(r"c:\Users\Amanda\Downloads\vendas_tratado.csv", index=False)
    print("Arquivo tratado salvo com sucesso!")

# Execução principal
if __name__ == "__main__":
    CAMINHO_CSV = r"c:\Users\Amanda\Downloads\Viana Maybelle(Planilha1).csv"

    observer = Observer()
    handler = ExcelChangeHandler()

    observer.schedule(handler, path=r"c:\Users\Amanda\Downloads", recursive=False)
    observer.start()

    print("Monitorando alterações em arquivos CSV... Pressione Ctrl+C para parar.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()
        print("Monitoramento encerrado.")
