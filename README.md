# data-platform-360 — VarejoMais S.A.

Plataforma de dados omnichannel construída com **Databricks + Claude Code + AI Dev Kit**.  
Unifica dados de PDV, e-commerce, SAP e rede adquirida em uma arquitetura Medallion (Bronze → Silver → Gold) com orquestração por Workflow e dashboard AI/BI.

---

## Índice

1. [Contexto do Caso](#contexto-do-caso)
2. [Pré-requisitos](#pré-requisitos)
3. [Estrutura do Projeto](#estrutura-do-projeto)
4. [Configuração do Ambiente](#configuração-do-ambiente)
5. [Asset Bundle (databricks.yml)](#asset-bundle-databricksyml)
6. [Unity Catalog — Schemas e Volumes](#unity-catalog--schemas-e-volumes)
7. [Dados de Origem](#dados-de-origem)
8. [Arquitetura Medallion](#arquitetura-medallion)
9. [Notebooks por Camada](#notebooks-por-camada)
10. [Job de Orquestração](#job-de-orquestração)
11. [Dashboard AI/BI](#dashboard-aibi)
12. [Comandos Comuns](#comandos-comuns)
13. [Volumes e Row Counts](#volumes-e-row-counts)

---

## Contexto do Caso

A VarejoMais S.A. opera com **silos de dados**: PDV em CSV separado por ponto-e-vírgula, pedidos de e-commerce sem CPF, eventos de app em JSONL, master de produtos no SAP e transações de rede adquirida em planilha despadronizada. Não existe chave comum entre os sistemas.

O objetivo do projeto é:
- **Unificar clientes** via normalização de CPF (11 dígitos) como chave de identidade
- **Mapear produtos** entre SKU de loja, SKU de e-commerce e código SAP
- **Consolidar pedidos** dos três canais de venda (PDV, e-commerce, rede adquirida)
- Disponibilizar uma **Visão 360° do cliente** para analytics e dashboards

---

## Pré-requisitos

### Ferramentas locais

| Ferramenta | Versão mínima | Instalação |
|------------|--------------|------------|
| Python | 3.11+ | [python.org](https://python.org) |
| [uv](https://docs.astral.sh/uv/) | 0.4+ | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| [Databricks CLI v2](https://docs.databricks.com/dev-tools/cli/databricks-cli.html) | 0.220+ | `brew install databricks/tap/databricks` |

### Configuração do perfil Databricks CLI

```bash
databricks configure --profile DEFAULT
# Host: https://dbc-79f7f0ef-6d29.cloud.databricks.com
# Token: <seu PAT>
```

Verificar autenticação:

```bash
databricks auth env --profile DEFAULT
```

### AI Dev Kit (MCP Server + Skills)

O projeto usa o [Databricks AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit), que fornece skills e um servidor MCP para o Claude Code:

```bash
# Instalar dependências Python (inclui databricks-tools-core)
uv sync

# Instalar skills no projeto local
./databricks-skills/install_skills.sh --local
```

O servidor MCP está configurado em `.mcp.json` e é carregado automaticamente pelo Claude Code.

### Unity Catalog

O catalog `data-platform` deve existir no workspace antes do primeiro deploy. Caso não exista:

```sql
CREATE CATALOG IF NOT EXISTS `data-platform`;
```

O schema `default` e o volume `landing-zone-varejo` (com os arquivos CSV/JSONL de origem) devem estar disponíveis em:

```
/Volumes/data-platform/default/landing-zone-varejo/
```

---

## Estrutura do Projeto

```
data-platform-360/
├── databricks.yml                        # Bundle principal — targets dev e prod
├── pyproject.toml                        # Dependências Python (uv)
├── CLAUDE.md                             # Instruções para o Claude Code
├── resources/
│   └── jobs/
│       └── job_visao_360_daily.yml       # Definição do job diário
└── src/
    ├── transformations/                  # Funções reutilizáveis
    │   ├── __init__.py
    │   ├── cpf_utils.py                  # normalize_cpf(), validate_cpf()
    │   └── bronze_transforms.py          # Helpers de ingestão
    └── notebooks/
        ├── 00_setup_schemas.py           # Cria schemas e volume de checkpoints
        ├── bronze/
        │   ├── 01_bronze_pdv_lojas.py
        │   ├── 02_bronze_ecommerce_pedidos.py
        │   ├── 03_bronze_app_eventos.py
        │   ├── 04_bronze_sap_clientes.py
        │   ├── 05_bronze_sap_estoque.py
        │   └── 06_bronze_rede_adquirida.py
        ├── silver/
        │   ├── 07_silver_dim_produto.py
        │   ├── 08_silver_clientes_unificados.py
        │   └── 09_silver_pedidos_consolidados.py
        └── gold/
            ├── 10_gold_visao_360_clientes.py
            ├── 11_gold_vendas_omnichannel.py
            └── 12_gold_base_previsao_demanda.py
```

---

## Configuração do Ambiente

```bash
# 1. Clonar o repositório
git clone <url-do-repo>
cd data-plataform-varejomais

# 2. Instalar dependências
uv sync

# 3. Validar o bundle
databricks bundle validate

# 4. Deploy no ambiente dev
databricks bundle deploy -t dev
```

### Variáveis de ambiente (opcional)

Para evitar prompts interativos, exporte:

```bash
export DATABRICKS_HOST=https://dbc-79f7f0ef-6d29.cloud.databricks.com
export DATABRICKS_TOKEN=<seu-token>
```

---

## Asset Bundle (databricks.yml)

O arquivo `databricks.yml` na raiz define o **Databricks Asset Bundle** com dois targets:

```yaml
bundle:
  name: data-platform-360

include:
  - resources/jobs/*.yml

targets:
  dev:
    mode: development      # prefixo "[dev <user>]" nos recursos
    default: true
    workspace:
      host: https://dbc-79f7f0ef-6d29.cloud.databricks.com
      profile: DEFAULT
  prod:
    workspace:
      host: https://dbc-79f7f0ef-6d29.cloud.databricks.com
      profile: DEFAULT
```

O modo `development` garante que os recursos fiquem isolados por usuário (ex: `[dev joaoh_soaresp] job-visao-360-daily`) sem interferir com outros desenvolvedores ou com o ambiente `prod`.

### Comandos do bundle

```bash
databricks bundle validate             # Valida o YAML sem fazer deploy
databricks bundle deploy -t dev        # Deploy no ambiente dev
databricks bundle deploy -t prod       # Deploy em produção (requer confirmação)
databricks bundle run job_visao_360_daily -t dev  # Executa o job manualmente
databricks bundle destroy -t dev       # Remove os recursos do workspace
```

---

## Unity Catalog — Schemas e Volumes

O notebook `00_setup_schemas.py` é a primeira task do job e cria todos os schemas idempotentemente:

| Schema | Finalidade |
|--------|-----------|
| `data-platform.bronze_pdv` | Dados crus do PDV (lojas físicas) |
| `data-platform.bronze_ecommerce` | Pedidos do canal e-commerce |
| `data-platform.bronze_app` | Eventos comportamentais do app mobile |
| `data-platform.bronze_sap` | Master de clientes e estoque do SAP |
| `data-platform.bronze_rede_adquirida` | Transações da rede adquirida |
| `data-platform.silver` | Dados limpos, deduplicados e tipados |
| `data-platform.gold` | Modelos agregados prontos para consumo |

Também cria o volume de checkpoints:
```
/Volumes/data-platform/default/checkpoints/
```

---

## Dados de Origem

Todos os arquivos ficam em `/Volumes/data-platform/default/landing-zone-varejo/`:

| Arquivo | Formato | Particularidades |
|---------|---------|-----------------|
| `01_pdv_lojas.csv` | CSV `;` | CPF com máscara `XXX.XXX.XXX-XX`, valores decimais com vírgula BR |
| `02_ecommerce_pedidos.csv` | CSV `,` | Sem CPF — chave: `customer_id` (`CUST-xxx`) |
| `03_app_eventos.jsonl` | JSONL | CPF parcialmente mascarado, `event_time` ISO 8601 |
| `04_sap_clientes.csv` | CSV `,` | CPF sem máscara (11 dígitos), `DATA_NASC` formato `YYYYMMDD` |
| `04_sap_estoque.csv` | CSV `,` | Código SAP (`COD_SAP`), estoque por loja |
| `05_rede_adquirida_planilha.csv` | CSV `,` | Datas mistas (`DD/MM/YYYY` e `DD-MM-YY`), valores com prefixo `R$ ` |

---

## Arquitetura Medallion

```
Landing Zone (Volume)
        │
        ▼
   ┌─────────┐   spark.read batch    ┌────────────────────┐
   │  Bronze  │ ──────────────────── │ 6 tabelas Delta    │
   │          │   replaceWhere       │ particionadas por  │
   │          │   (idempotente)      │ ingestion_dt       │
   └─────────┘                       └────────────────────┘
        │
        ▼
   ┌─────────┐   MERGE INTO          ┌────────────────────┐
   │  Silver  │ ──────────────────── │ dim_produto        │
   │          │   normalização CPF   │ clientes_unificados│
   │          │   match produtos     │ pedidos_consolidados│
   └─────────┘                       └────────────────────┘
        │
        ▼
   ┌─────────┐   overwrite diário    ┌────────────────────┐
   │   Gold   │ ──────────────────── │ visao_360_clientes │
   │          │                      │ vendas_omnichannel │
   │          │                      │ base_previsao_     │
   └─────────┘                       │ demanda            │
                                     └────────────────────┘
```

### Convenções de código

- **Linguagem**: PySpark Python 3.11. SQL apenas em views e consultas analíticas.
- **Nomes de tabelas**: `snake_case`. Prefixo por camada quando fora do Unity Catalog (`tb_bronze_`, `tb_silver_`, `tb_gold_`).
- **Nomes de colunas**: `snake_case`, sem acentos. Sufixo `_ts` (timestamp) ou `_dt` (date).
- **Formato**: sempre Delta Lake. Nunca Parquet cru para tabelas gerenciadas.
- **Particionamento**: Bronze e Silver particionados por `ingestion_dt`. Gold sem particionamento.
- **Segredos**: nunca hardcoded — usar `dbutils.secrets.get()` ou perfis da Databricks CLI.
- **Estilo**: formatado com `black`, imports ordenados com `isort`, lint com `ruff` (linha 120).

---

## Notebooks por Camada

### Setup (`00_setup_schemas.py`)

Cria todos os 7 schemas e o volume de checkpoints. Idempotente — pode ser executado múltiplas vezes sem efeitos colaterais.

---

### Bronze — Ingestão Batch

Padrão aplicado em todos os 6 notebooks Bronze:

```python
from datetime import date
from pyspark.sql.functions import col, current_date, current_timestamp

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", ";")           # ajustado por arquivo
    .option("inferSchema", "true")
    .option("mode", "PERMISSIVE")
    .load(VOLUME_PATH)
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("ingestion_dt", current_date())
    .withColumn("source_file", col("_metadata.file_path"))  # rastreabilidade
)

df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"ingestion_dt = '{date.today()}'")  # idempotência diária
    .option("mergeSchema", "true")
    .partitionBy("ingestion_dt")
    .saveAsTable(TARGET_TABLE)
```

> **Nota:** `input_file_name()` não é suportado no Unity Catalog com Serverless. Usa-se `col("_metadata.file_path")`.

| Notebook | Tabela gerada | Particularidade |
|----------|--------------|-----------------|
| `01_bronze_pdv_lojas.py` | `bronze_pdv.pdv_lojas` | Separador `;`, decimal BR |
| `02_bronze_ecommerce_pedidos.py` | `bronze_ecommerce.ecommerce_pedidos` | Separador `,`, timestamps ISO |
| `03_bronze_app_eventos.py` | `bronze_app.app_eventos` | Formato JSONL |
| `04_bronze_sap_clientes.py` | `bronze_sap.sap_clientes` | CPF sem máscara |
| `05_bronze_sap_estoque.py` | `bronze_sap.sap_estoque` | Código SAP por loja |
| `06_bronze_rede_adquirida.py` | `bronze_rede_adquirida.rede_adquirida` | `inferSchema=false` (dados inconsistentes), renomeação snake_case |

---

### Silver — Limpeza e Unificação

#### `07_silver_dim_produto.py` → `silver.dim_produto`

- Granularidade: **1 linha por produto único**
- Une produtos de PDV (`PRODUTO`), e-commerce (`product_name`) e SAP (`DESCRICAO`)
- Normaliza nomes: `UPPER(TRIM(REGEXP_REPLACE(nome, '[^a-zA-Z0-9 ]', '')))`
- `produto_id = SHA2(nome_normalizado, 256)`
- Mapeia `sku_loja`, `sku_ecom`, `cod_sap` para a mesma entidade
- Idempotência: `MERGE INTO` na chave `produto_id`

#### `08_silver_clientes_unificados.py` → `silver.clientes_unificados`

- Granularidade: **1 linha por cliente único**
- Chave de unificação: CPF normalizado para 11 dígitos via `normalize_cpf()`
- Estratégia por fonte:
  - **PDV**: `normalize_cpf(CPF)` → `cliente_unico_id = cpf_normalizado`
  - **SAP**: CPF já em 11 dígitos → enriquece com `segmento_sap`, `cidade`, `uf`, `data_nasc_dt`
  - **Rede adquirida**: `normalize_cpf(documento)` quando disponível; caso contrário `REDE-<hash>`
  - **E-commerce**: sem CPF → `cliente_unico_id = 'ECOM-' + customer_id`
- Idempotência: `MERGE INTO` na chave `cliente_unico_id`

#### `09_silver_pedidos_consolidados.py` → `silver.pedidos_consolidados`

- Granularidade: **1 linha por item de pedido**
- UNION de 3 fontes com schema unificado
- Tratamento especial por canal:
  - **PDV**: `VLR_TOTAL` com vírgula BR → `regexp_replace + cast(DecimalType(12,2))`
  - **E-commerce**: `unit_price × quantity` com cast explícito
  - **Rede adquirida**: valores com prefixo `R$ ` + datas mistas (`DD/MM/YYYY` e `DD-MM-YY`) via `try_to_date()` (SQL expr)
- `pedido_id = SHA2(chaves_naturais, 256)`
- Idempotência: overwrite completo com `overwriteSchema=true`

---

### Gold — Modelos de Negócio

#### `10_gold_visao_360_clientes.py` → `gold.visao_360_clientes`

- Granularidade: **1 linha por cliente**
- Junta `silver.clientes_unificados` + métricas de `silver.pedidos_consolidados` + eventos de `bronze_app.app_eventos`
- Colunas: `total_pedidos`, `valor_total_compras`, `ticket_medio`, `pedidos_pdv`, `pedidos_ecommerce`, `pedidos_rede_adquirida`, `primeira_compra_dt`, `ultima_compra_dt`, `qtd_sessoes_app`, `dispositivo_preferido`
- Idempotência: `mode("overwrite")` diário

#### `11_gold_vendas_omnichannel.py` → `gold.vendas_omnichannel`

- Granularidade: **1 linha por (canal, categoria, mes_ano)**
- Agrega `silver.pedidos_consolidados` com `silver.dim_produto`
- Colunas: `qtd_pedidos`, `receita_total`, `ticket_medio`, `clientes_unicos`
- Idempotência: `mode("overwrite")` diário

#### `12_gold_base_previsao_demanda.py` → `gold.base_previsao_demanda`

- Granularidade: **1 linha por (produto, loja, data_pedido_dt)**
- Cruza vendas do canal PDV com estoque do SAP
- Colunas: `qtd_vendida`, `estoque_atual`, `estoque_minimo`, `custo_medio`
- Idempotência: `mode("overwrite")` diário

---

## Job de Orquestração

**Nome**: `job-visao-360-daily`  
**Schedule**: diariamente às 07:00 BRT (`0 0 7 * * ?`, fuso `America/Sao_Paulo`)  
**Compute**: Serverless (`client: "2"` — Spark Connect)  
**Timeout**: 1 hora  
**Notificação**: e-mail em caso de falha → `joaoh.soaresp@gmail.com`

### DAG de tarefas

```
setup_schemas
    │
    ├── bronze_pdv ──────────┐
    ├── bronze_ecom ─────────┤── silver_produtos ──┐
    ├── bronze_app           │                     │
    ├── bronze_sap_clientes ─┤                     ├── silver_pedidos ──┬── gold_visao_360
    ├── bronze_sap_estoque ──┘── silver_clientes ──┘                   ├── gold_vendas
    └── bronze_rede ─────────────────────────────────────────────────► └── gold_demanda
```

As tasks Bronze rodam em **paralelo** após o setup. Silver aguarda as Bronze relevantes. Gold aguarda Silver.

### Deploy e execução

```bash
# Deploy
databricks bundle deploy -t dev

# Executar manualmente
databricks bundle run job_visao_360_daily -t dev

# Listar runs recentes
databricks jobs list --output json | jq '.[].settings.name'
```

---

## Dashboard AI/BI

**Nome**: Visão 360 - Clientes VarejoMais  
**URL**: `https://dbc-79f7f0ef-6d29.cloud.databricks.com/sql/dashboardsv3/01f165e352cf10469f70c8c3122cc553`

### Widgets

| Widget | Tipo | Dataset | Métrica |
|--------|------|---------|---------|
| Total de Clientes | Counter | `gold.visao_360_clientes` | `COUNT(DISTINCT cliente_unico_id)` |
| Receita Total | Counter | `gold.visao_360_clientes` | `SUM(valor_total_compras)` |
| Ticket Médio | Counter | `gold.visao_360_clientes` | `AVG(ticket_medio)` |
| Receita por Canal | Bar chart | `gold.vendas_omnichannel` | `SUM(receita_total)` por canal |
| Clientes por Segmento | Pie chart | `gold.visao_360_clientes` | `COUNT(*)` por `segmento_sap` |
| Receita Mensal por Canal | Line chart | `gold.vendas_omnichannel` | `SUM(receita_total)` por mês e canal |
| Receita por Categoria | Bar horizontal | `gold.vendas_omnichannel` | `SUM(receita_total)` por categoria |
| Top 20 Clientes | Tabela | `gold.visao_360_clientes` | Ordenado por `valor_total_compras DESC` |

O dashboard foi criado seguindo o workflow obrigatório: schemas → queries → `execute_sql()` (validação) → JSON → deploy → publish.

---

## Comandos Comuns

```bash
# Ambiente local
uv sync                                   # Instalar dependências
black src/ && isort src/                  # Formatar código
ruff check src/                           # Lint
pytest tests/ -v                          # Rodar testes unitários

# Databricks CLI
databricks bundle validate                # Validar YAML
databricks bundle deploy -t dev           # Deploy dev
databricks bundle deploy -t prod          # Deploy prod (requer confirmação)
databricks bundle run job_visao_360_daily -t dev  # Execução manual

# AI Dev Kit
./databricks-skills/install_skills.sh --local    # Reinstalar skills localmente
```

---

## Volumes e Row Counts

Resultado após execução bem-sucedida do job (validado em 2026-06-11):

| Camada | Tabela | Linhas |
|--------|--------|-------:|
| Bronze | `bronze_pdv.pdv_lojas` | 22.000 |
| Bronze | `bronze_ecommerce.ecommerce_pedidos` | 18.000 |
| Bronze | `bronze_app.app_eventos` | 40.000 |
| Bronze | `bronze_sap.sap_clientes` | 2.400 |
| Bronze | `bronze_sap.sap_estoque` | 1.227 |
| Bronze | `bronze_rede_adquirida.rede_adquirida` | 8.000 |
| Silver | `silver.dim_produto` | 400 |
| Silver | `silver.clientes_unificados` | 4.152 |
| Silver | `silver.pedidos_consolidados` | 48.000 |
| Gold | `gold.visao_360_clientes` | 4.152 |
| Gold | `gold.vendas_omnichannel` | 504 |
| Gold | `gold.base_previsao_demanda` | 21.989 |

Query de validação:

```sql
SELECT 'bronze_pdv'         AS tabela, COUNT(*) AS linhas FROM `data-platform`.bronze_pdv.pdv_lojas
UNION ALL SELECT 'bronze_ecom',         COUNT(*) FROM `data-platform`.bronze_ecommerce.ecommerce_pedidos
UNION ALL SELECT 'bronze_app',          COUNT(*) FROM `data-platform`.bronze_app.app_eventos
UNION ALL SELECT 'bronze_sap_clientes', COUNT(*) FROM `data-platform`.bronze_sap.sap_clientes
UNION ALL SELECT 'bronze_sap_estoque',  COUNT(*) FROM `data-platform`.bronze_sap.sap_estoque
UNION ALL SELECT 'bronze_rede',         COUNT(*) FROM `data-platform`.bronze_rede_adquirida.rede_adquirida
UNION ALL SELECT 'silver_dim_produto',  COUNT(*) FROM `data-platform`.silver.dim_produto
UNION ALL SELECT 'silver_clientes',     COUNT(*) FROM `data-platform`.silver.clientes_unificados
UNION ALL SELECT 'silver_pedidos',      COUNT(*) FROM `data-platform`.silver.pedidos_consolidados
UNION ALL SELECT 'gold_visao_360',      COUNT(*) FROM `data-platform`.gold.visao_360_clientes
UNION ALL SELECT 'gold_vendas',         COUNT(*) FROM `data-platform`.gold.vendas_omnichannel
UNION ALL SELECT 'gold_demanda',        COUNT(*) FROM `data-platform`.gold.base_previsao_demanda
ORDER BY 1;
```
