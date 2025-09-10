# 🍷 ETL Pipeline para Análise de Qualidade de Vinhos  

Este repositório contém uma pipeline de **ETL (Extract, Transform, Load)** desenvolvida para analisar a qualidade do vinho branco de uma fonte de dados fornecido previamente.  
O projeto foi implementado em **Databricks** utilizando uma sequência de *notebooks* em Python e é orquestrado como um **Databricks Job**.  

A arquitetura segue o padrão **Medallion (Bronze, Silver, Gold)**, refinando os dados progressivamente do estado bruto até gerar insights prontos para análise.  

---

## 📂 Estrutura do Projeto  

A pipeline é dividida em etapas, cada uma representada por um *notebook* executado no Databricks:  

- **00_setup_env.py** → Cria e configura o banco de dados.  
- **01_bronze_ingest.py** → Ingere os dados brutos na camada Bronze.  
- **02_silver_transform.py** → Limpa e transforma os dados para a camada Silver.  
- **03_gold_kpis.py** → Agrega os dados e gera KPIs na camada Gold.  
- **04_analytics_validate.py** → Valida os resultados e executa análises exploratórias.  
- **Wine_Quality_Job.yaml** → Define a orquestração da pipeline no Databricks.  

---

## 🏗️ Arquitetura Medallion  

O fluxo de dados percorre três camadas:  

### 🔹 Bronze (Dados Brutos)  
- Fonte: `dbfs:/databricks-datasets/wine-quality/winequality-white.csv`  
- Operações:  
  - Leitura do CSV com schema explícito.  
  - Padronização dos nomes de colunas.  
  - Persistência em Delta Table: `wine_db.df_bronze`.  

### 🔸 Silver (Dados Tratados e Enriquecidos)  
- Fonte: `wine_db.df_bronze`  
- Operações:  
  - Remoção de duplicados.  
  - Criação da coluna categórica `quality_label`.
  - Aplicação de filtros de integridade.
  - Persistência em Delta Table: `wine_db.df_silver`.  

### 🟡 Gold (KPIs Agregados)  
- Fonte: `wine_db.df_silver`  
- Operações:  
  - Agrupamento por `quality_label`.  
  - Cálculo de métricas (KPIs):  
    - Média de álcool,
    - Média de açúcar residual,
    - Média de densidade,
    - pH médio arredondado,
    - Desvio padrão do álcool,
    - Contagem total de vinhos,
  - Persistência em Delta Table: `wine_db.df_gold`.  

---

## 📊 Validação e Análises  

No *notebook* `04_analytics_validate.py`:  
- Exibição da tabela Gold e seu schema final.  
- Comparação da evolução do schema (Bronze → Gold).  
- Insights sobre distribuição de qualidade e variabilidade de atributos.  

---

## ⚙️ Orquestração  

A execução automatizada é definida no arquivo `Wine_Quality_Job.yaml`, que cria o job **Wine Quality Job** no Databricks.  
Ordem das tarefas:  

1. 00_setup_env  
2. 01_bronze_ingest  
3. 02_silver_transform  
4. 03_gold_kpis  
5. 04_analytics_validate  

---

## 🚀 Como Executar  

1. Importe os *notebooks* (`.py`) e o arquivo de configuração (`.yaml`) no seu **Databricks Workspace**.  
2. Utilize o dataset público já disponível em: dbfs:/databricks-datasets/wine-quality/winequality-white.csv
*(não é necessário upload de dados)*  
3. Execute manualmente os *notebooks* em sequência (00 → 04) ou rode o job completo `Wine Quality Job`.  

---
