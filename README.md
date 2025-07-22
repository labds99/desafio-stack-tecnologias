# desafio-stack-tecnologias

## 🙋 Sobre Mim

👨‍💻 Leonardo Souza  
Engenheiro de Dados  
📧 leo.andrade.bsouza@gmail.com  
🔗 [linkedin.com/in/]()

# 🛠️ Desafio Técnico - Stack Tecnologias

Este repositório contém a solução desenvolvida por mim, Leonardo Souza, para o desafio técnico da Stack Tecnologias. A proposta foi construir um pipeline de dados moderno e completo, utilizando os principais serviços do ecossistema AWS, com foco em boas práticas de engenharia de dados, segurança, versionamento e governança.

---

## 📌 Objetivo do Projeto

Construir um pipeline de ingestão, transformação e exposição de dados em arquitetura de Data Lake, aplicando:

- PySpark com Delta Lake no AWS Glue
- Organização em camadas Medallion (Bronze, Silver, Gold)
- Catalogação com Glue Data Catalog
- Particionamento e incrementalidade
- Carga final em banco (PostgreSQL)
- Orquestração com AWS Step Functions
- Segurança com IAM e SSE-KMS
- Organização escalável no S3

---

## 🧱 Arquitetura do Projeto

### 🔷 Medallion Architecture

| Camada | Objetivo | Formato | Qualidade dos dados |
|--------|----------|---------|----------------------|
| Dropzone | Área de entrada dos dados brutos | CSV | Sem garantia |
| Bronze   | Dados técnicos com metadados e particionamento | Delta Lake | Raw confiável |
| Silver   | Dados otimizados com merge/upsert e deduplicação | Delta Lake | Pronto para consumo técnico |
| Gold     | Dados agregados para análise de negócio | Delta Lake | Pronto para BI e relatórios |

---

## 🪣 Buckets Utilizados

Todos seguem as boas práticas: versionamento, bloqueio de acesso público e criptografia SSE-KMS.

| Bucket | Finalidade |
|--------|------------|
| `leonardo-souza-dropzone-dev-us-east-1` | Entrada de arquivos CSV brutos |
| `leonardo-souza-bronze-dev-us-east-1`   | Dados convertidos para Delta, com metadados |
| `leonardo-souza-silver-dev-us-east-1`   | Dados deduplicados e com merge incremental |
| `leonardo-souza-gold-dev-us-east-1`     | Dados agregados por período e tipo de pagamento |

---

## ⚙️ Componentes e Processamento

### 🔸 Bronze Job (ETL)
- Leitura de CSV da Dropzone
- Inclusão de colunas técnicas: `ingestion_timestamp`, `source_file`, `year`, `month`
- Escrita como Delta com particionamento
- Criação de tabela externa no Glue Catalog:
  - `bronze_layer_manual_files.kaggle_nyc_yellow_taxi_trip`

### 🔸 Silver Job (Delta Merge)
- Leitura da tabela Bronze
- Cast de colunas, validação de datas e filtros mínimos
- Deduplicação por chave composta
- Escrita incremental com **merge/upsert**
- Registro no Glue Catalog:
  - `silver_layer_manual_files.kaggle_nyc_taxi_trip`

### 🔸 Gold Job (Agregações)
- Leitura da Silver
- Agregação por `year`, `month` e `payment_type`
- Métricas calculadas: total de corridas, total arrecadado
- Escrita em Delta com **merge/upsert**
- Registro no Glue Catalog:
  - `gold_layer_manual_files.kaggle_nyc_taxi_trip`

---

## 🧠 Estratégias adotadas

- **Partition Discovery**: `year` e `month` para facilitar queries otimizadas
- **Delta Lake**: usado em todas as camadas para permitir versionamento e merge eficiente
- **Upsert (MERGE)**: implementado nas camadas Silver e Gold
- **Job modularizado**: com funções bem separadas para `extract`, `transform`, `merge` e `catalog`
- **Log estruturado**: via `logger` em todas as camadas

---

## 🔐 Segurança e Acesso

- IAM User pessoal criado com acesso restrito (sem uso de root)
- IAM Role exclusiva para o Glue com política customizada
- Criptografia dos buckets com SSE-KMS
- Conector JDBC configurado com segurança para acesso ao PostgreSQL no RDS
- Banco de dados RDS não exposto publicamente

---

## 📷 Evidências da Execução

Inclui screenshots anexadas:

- Conexão com PostgreSQL no Glue
- Execução bem-sucedida dos jobs Bronze, Silver e Gold
- Catálogo de tabelas no Glue
- Orquestração com AWS Step Functions

---

## 🚀 Próximos Passos

- Implementação de monitoramento com CloudWatch/SNS
- Exportação dos dados da Gold para dashboards ou consultas BI via RDS/Redshift

---

## 🗂️ Estrutura do Repositório

desafio-stack-tecnologias/
├── captura_de_tela_aws/
│   ├── aurora_rds_postgresql_connection.png
│   ├── bronze_job_execution.png
│   ├── connector_rds_postgresql_to_glue.png
│   ├── glue_studio_jobs.png
│   ├── gold_job_execution.png
│   ├── silver_job_execution.png
│   └── step_functions.png
├── data_ingestion/
│   ├── bronze_job.py
│   ├── silver_job.py
│   └── gold_job.py
├── README.md
