# Databricks AI Dev Kit — Caso VarejoMais S.A.

## Contexto do projeto
Bases de Dados — Estudo de Caso VarejoMais S.A.

Conjunto de dados sintéticos que reproduz os dois problemas centrais do caso:
silos de dados e incapacidade de transformar dados em insights. Os dados
foram desenhados para que a unificação exija engenharia de dados real (matching de
clientes/produtos, limpeza, padronização) — não há uma chave comum pronta.

Workspace principal para desenvolvimento de projetos Databricks com Claude Code.
Contém skills instaladas para todos os domínios Databricks e um MCP server configurado.

- **Nome**:  data-platform-360 (plataforma de dados de clientes)
- **Objetivo**: ingestão, transformação e disponibilização de dados de clientes para dashboards
- **Stack**: Databricks (Unity Catalog), PySpark, Delta Lake
- **Orquestração**: Databricks Workflows (Jobs) / Spark Declarative Pipelines.
-**Catalog:** Criar Catalog de nome data-platform

- **Arquitetura**: Medallion (Bronze → Silver → Gold).

    - ***Bronze***: dados crus, ingestão append-only, sem transformação. (Organizar as tabelas por sistema de origem (schema))
    - ***Silver***: dados limpos, deduplicados, tipados, com chaves de negócio.
    - ***Gold***: modelos agregados/dimensionais prontos para consumo (1 linha por cliente nas tabelas de visão 360).
    - ***Catálogos Unity Catalog***: dev, staging, prod — nunca escrever direto em prod sem revisão.

- **Workspace**: https://dbc-79f7f0ef-6d29.cloud.databricks.com
- **Profile CLI**: DEFAULT
- **MCP Server**: Databricks MCP em .mcp.json (carregado automaticamente)

## OBRIGATÓRIO: Regras de Invocação de Skills

Antes de escrever qualquer código, configuração ou consulta, identifique o domínio e invoque a skill correspondente. Nunca pule este passo.
NÃO USAR SDP - Spark Declarative Pipelines


1. **Bronze** — ingerir os 6 arquivos como estão (Auto Loader / `spark.read`),
   preservando o bruto. Repare como cada um exige opções diferentes
   (`sep=";"`, `multiLine`, schema do JSON, etc.).
2. **Silver** — limpar e padronizar:
   - normalizar CPF (remover máscara → 11 dígitos) como chave de unificação;
   - padronizar datas, valores e nomes;
   - resolver identidade do cliente → `cliente_unico_id`;
   - mapear produtos (SKU loja ↔ SKU ecom ↔ COD_SAP) para um `produto_id` único.
3. **Gold** — tabelas de negócio:
   - **Visão 360° do cliente** (compras loja + ecom + comportamento app);
   - **Vendas omnichannel** por categoria/loja/mês;
   - **Base para previsão de demanda** cruzando vendas com estoque.
4. **Validação** — confira a unificação contra `_gabarito_clientes.csv`
   (quantos CPFs você conseguiu casar corretamente entre os sistemas).

5. **Dashboard AI/BI - VarejoMais - Visão 360 Clientes** 
**Pré-requisitos comuns:** unificar cliente pelo CPF normalizado (11 dígitos) → `cliente_unico_id`; mapear produtos entre `SKU loja ↔ product_sku ecom ↔ COD_SAP`; padronizar valores e datas na camada Silver.

### Pipelines e Ingestão de Dados

| Cenário | Skill |
|---------|-------|
| Criar/editar pipeline com notebooks (bronze/silver/gold), Auto Loader  | `spark-python-data-source` |
| Criar/editar pipeline de streaming com Python/Scala (Structured Streaming) | `spark-python-data-source` |
| Ingestão de dados externos, conectores, fontes customizadas Python | `spark-python-data-source` |
| Ingestão via ZeroBus / event-driven ingestion | `databricks-zerobus-ingest` |

### Bundle, Jobs e Orquestração

| Cenário | Skill |
|---------|-------|
| Criar/editar `databricks.yml`, `resources/*.yml`, deploy de bundle (DABs) | `databricks-bundles` |
| Criar/editar jobs, schedules, tasks, email notifications | `databricks-jobs` |

### Dashboards e Analytics

| Cenário | Skill |
|---------|-------|
| Criar/editar dashboard AI/BI (`.lvdash.json`), widgets, KPIs, filtros | `databricks-aibi-dashboards` |
| Criar/editar Genie Space, perguntas em linguagem natural sobre dados | `databricks-genie` |
| Criar/editar Metric Views, métricas reutilizáveis | `databricks-metric-views` |
| Queries SQL, SQL Warehouse, DBSQL | `databricks-dbsql` |

### Unity Catalog e Governança

| Cenário | Skill |
|---------|-------|
| Criar/gerenciar catalogs, schemas, volumes, grants, tabelas | `databricks-unity-catalog` |
| Trabalhar com tabelas Iceberg, open table formats | `databricks-iceberg` |

### Machine Learning e MLflow

| Cenário | Skill |
|---------|-------|
| Primeiros passos com MLflow no Databricks | `mlflow-onboarding` |
| Instrumentar código com MLflow Tracing (spans, traces) | `instrumenting-with-mlflow-tracing` |
| Avaliar modelos e experimentos MLflow | `databricks-mlflow-evaluation` |
| Consultar métricas e runs MLflow | `querying-mlflow-metrics` |
| Buscar e recuperar traces MLflow | `retrieving-mlflow-traces` |
| Analisar traces de sessão de chat | `analyze-mlflow-chat-session` |
| Analisar um trace MLflow específico | `analyze-mlflow-trace` |
| Buscar documentação MLflow | `searching-mlflow-docs` |
| Servir modelos (Model Serving endpoints) | `databricks-model-serving` |

### AI e Agentes

| Cenário | Skill |
|---------|-------|
| Construir agentes com Mosaic AI Agent Bricks | `databricks-agent-bricks` |
| Avaliar agentes (Agent Evaluation) | `agent-evaluation` |
| Usar AI Functions no SQL (`ai_query`, `ai_generate_text`) | `databricks-ai-functions` |
| Vector Search (índices, embeddings, busca semântica) | `databricks-vector-search` |

### Apps e SDK

| Cenário | Skill |
|---------|-------|
| Criar Databricks App (dashboard/UI) com Python | `databricks-apps-python` |
| Criar Databricks App com APX framework | `databricks-app-apx` |
| Gerenciar apps via CLI/SDK | `databricks-apps` |
| Usar Databricks Python SDK | `databricks-python-sdk` |

### Dados e Infraestrutura

| Cenário | Skill |
|---------|-------|
| Lakebase (PostgreSQL compatível no Databricks) | `databricks-lakebase` |
| Lakebase com autoscaling | `databricks-lakebase-autoscale` |
| Lakebase provisionado | `databricks-lakebase-provisioned` |
| Geração de dados sintéticos para testes/demos | `databricks-synthetic-data-gen` |
| Geração e upload de PDFs não estruturados | `databricks-unstructured-pdf-generation` |
| Configurar profiles, autenticação CLI | `databricks-config` |

---

## Gatilhos por Intenção do Usuário

Mapeamento de linguagem natural para skill:

- "pipeline", "bronze", "silver", "gold", "Auto Loader", "SDP", "streaming table" → `spark-python-data-source`
- "dashboard", "gráfico", "KPI", "widget", "counter", "chart", "filtro global" → `databricks-aibi-dashboards`
- "bundle", "deploy", "databricks.yml", "target prod" → `databricks-bundles`
- "job", "schedule", "agendamento", "task", "trigger" → `databricks-jobs`
- "catalog", "schema", "volume", "grants", "Unity Catalog" → `databricks-unity-catalog`
- "agente", "Agent Bricks", "RAG", "compound AI" → `databricks-agent-bricks`
- "model serving", "endpoint", "inferência" → `databricks-model-serving`
- "vector search", "embedding", "busca semântica" → `databricks-vector-search`
- "MLflow", "experimento", "run", "trace", "log" → skill MLflow correspondente ao objetivo
- "app", "Streamlit", "Dash", "interface web" → `databricks-apps-python`
- "dados sintéticos", "dados de teste", "demo data" → `databricks-synthetic-data-gen`
- "Genie", "perguntar sobre dados", "NL2SQL" → `databricks-genie`

---

## Workflow Obrigatório para Dashboards AI/BI

NUNCA pular qualquer etapa:

```
1. get_table_stats_and_schema()   → conhecer schema e tipos das colunas
2. Escrever queries SQL dos datasets
3. execute_sql()                  → testar CADA query (se falhar, corrigir antes de continuar)
4. Construir o JSON do dashboard  → usar apenas campos validados no passo 3
5. manage_dashboard(action="create_or_update") → deploy
6. manage_dashboard(action="publish")          → publicar
```

## Databricks AI Dev Kit

Este projeto usa o Databricks AI Dev Kit (https://github.com/databricks-solutions/ai-dev-kit),
toolkit da Field Engineering que dá aos agentes de código (Claude Code, Cursor, etc.) skills,
servidor MCP e ferramentas executáveis com consciência nativa do workspace Databricks.


Componentes em uso:

databricks-skills/ — skills markdown que ensinam padrões Databricks (pipelines, jobs, dashboards, Unity Catalog).
databricks-mcp-server/ — servidor MCP expondo ferramentas executáveis (SQL, jobs, catálogo).
databricks-tools-core/ — biblioteca Python com funções de alto nível (ex.: execute_sql).



Sempre preferir as skills e ferramentas do AI Dev Kit a gerar código "de memória" — elas refletem os padrões oficiais e reduzem alucinação.
Permissões do Unity Catalog são respeitadas pelas ferramentas do kit; mesmo assim, seguir as regras de ambiente abaixo.
Skills ficam em ./.claude/skills localmente; podem ser customizadas/adicionadas para padrões internos do time.
Reinstalar/atualizar skills: ./databricks-skills/install_skills.sh --local (ver --help para opções).

## Convenções de código
 
- **Linguagem:** PySpark em Python 3.11. SQL apenas para views e consultas analíticas.
- **Documentação**: Inicio de cada notebook deve ter uma explicaçao do que o codigo vai fazer, listando as origens lidas e a tabela que será gerada.
- **Estilo:** seguir PEP 8; formatar com `black` e ordenar imports com `isort`. Lint com `ruff`.
- **Nomes de tabelas:** `snake_case`, prefixadas pela camada quando fora do Unity Catalog (`tb_bronze_`, `tb_silver_`, `tb_gold_`).
- **Nomes de colunas:** `snake_case`, sem acentos. Datas em UTC com sufixo `_ts` (timestamp) ou `_dt` (date).
- **Tabelas sempre em formato Delta.** Não usar Parquet cru para tabelas gerenciadas.
- **Idempotência:** transformações devem ser reexecutáveis sem duplicar dados (usar `MERGE` ou overwrite particionado).
- **Particionamento:** particionar Bronze/Silver por data de ingestão (`ingestion_dt`); evitar over-partitioning em Gold.
- **Segredos:** nunca hardcodar credenciais. Usar Databricks Secrets (`dbutils.secrets.get`) ou perfis da Databricks CLI.
- **Notebooks:** lógica reutilizável vai para módulos `.py` em `src/`; notebooks apenas orquestram e exploram.
- **Docstrings:** toda função de transformação deve documentar entrada, saída e granularidade da tabela resultante.


## Estrutura de pastas
 
data-platform-360/
├── databricks.yml          # OBRIGATÓRIO na raiz — define o bundle, include e targets
├── resources/
│   ├── jobs/               # definições de jobs (*.yml)
│   │   ├── ingestion_job.yml
│   │   └── transformation_job.yml
│   └── pipelines/          # definições de pipelines ETL/DLT (*.yml) — opcional
│       └── medallion_pipeline.yml
├── src/
│   ├── transformations/    # lógica reutilizável (bronze/silver/gold)
│   ├── utils/
│   └── notebooks/          # notebooks de orquestração e exploração
├── tests/                  # testes unitários (pytest + chispa)
├── conf/                   # parâmetros de negócio que não cabem nos targets
├── .claude/skills/         # skills do AI Dev Kit
├── pyproject.toml          # dependências (uv)
└── README.md

## Comandos comuns
 
```bash
# Ambiente local
uv sync                                # instalar dependências (gerenciador uv, usado pelo AI Dev Kit)
black src/ tests/ && isort src/ tests/ # formatar código
ruff check src/                        # lint
pytest tests/ -v                       # rodar testes unitários
 
# Databricks CLI / Asset Bundles
databricks bundle validate             # validar Asset Bundle
databricks bundle deploy -t dev        # deploy no ambiente dev
databricks bundle run <job_name> -t dev
databricks jobs list                   # listar jobs
 
# AI Dev Kit — skills
./databricks-skills/install_skills.sh --local          # instalar skills do checkout local
./databricks-skills/install_skills.sh --install-to-genie  # subir skills para o workspace (Genie Code)
```

## Regras de comportamento (sempre seguir)
 
- **Sempre** usar as skills e ferramentas do AI Dev Kit para tarefas Databricks (pipelines, jobs, dashboards, Unity Catalog).
- **Sempre** rodar `ruff` e `pytest` antes de propor um commit.
- **Sempre** validar schema da tabela de destino antes de escrever (evitar schema drift silencioso).
- **Nunca** escrever em catálogo `prod` sem confirmação explícita do usuário.
- **Nunca** fazer `DROP` ou `OVERWRITE` em tabelas Gold sem backup ou confirmação.
- Preferir `MERGE INTO` para upserts em Silver/Gold em vez de delete + insert.
- Ao criar nova transformação, incluir teste unitário correspondente em `tests/`.
- Comentar a granularidade de toda tabela Gold criada (ex.: "1 linha por cliente").
- Usar um PAT com escopo limitado para o MCP server; não usar tokens com permissões amplas em dev.