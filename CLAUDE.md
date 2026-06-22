# Data Platform 360 — VarejoMais S.A.

## Visão Geral do Projeto

- **Nome**: data-platform-360 (plataforma de dados de clientes 360°)
- **Objetivo**: ingestão, transformação e disponibilização de dados de clientes para dashboards analíticos
- **Stack**: Databricks (Unity Catalog), PySpark 3.11, Delta Lake
- **Orquestração**: Databricks Workflows (Jobs)
- **Catalog**: `data-platform`

## Workspace & Configuração

- **Workspace**: https://dbc-79f7f0ef-6d29.cloud.databricks.com
- **Profile CLI**: DEFAULT
- **MCP Server**: Databricks MCP em `.mcp.json` (carregado automaticamente)
- **AI Dev Kit**: skills em `.claude/skills/`; reinstalar com `./databricks-skills/install_skills.sh --local`

## Status Atual do Projeto

Projeto deployado e funcional:

- **Bronze**: 6 tabelas ingeridas (Auto Loader, Serverless), schema por sistema de origem
- **Silver**: 3 tabelas limpas e unificadas por CPF normalizado (`cliente_unico_id`)
- **Gold**: 3 tabelas analíticas (Visão 360 do cliente, Vendas omnichannel, Previsão de demanda)
- **Job**: `customer-360-daily` — diário às 7h, serverless, notificação por e-mail em falha
- **Dashboard AI/BI**: publicado no workspace

## Arquitetura Medallion

- **Bronze**: ingestão append-only, sem transformação. Schema por sistema de origem. Preservar dados crus exatamente como chegam.
- **Silver**: limpeza, deduplicação, tipagem e chaves de negócio. CPF normalizado (11 dígitos sem máscara) como `cliente_unico_id`. Mapeamento `SKU loja ↔ product_sku ecom ↔ COD_SAP` → `produto_id`.
- **Gold**: agregações e modelos dimensionais prontos para consumo. **1 linha por cliente** nas tabelas de visão 360.
- **Ambientes Unity Catalog**: `dev`, `staging`, `prod` — nunca escrever em `prod` sem confirmação.

## OBRIGATÓRIO: Invocação de Skills

Antes de escrever qualquer código, configuração ou consulta, identifique o domínio (pipelines, bundles/jobs, dashboards, Unity Catalog, MLflow, agentes, apps, Lakebase etc.) e invoque a skill correspondente do AI Dev Kit — cada skill já documenta seus próprios gatilhos e workflows obrigatórios. Nunca pule este passo.

## Convenções de Código

- **Linguagem**: PySpark em Python 3.11. SQL apenas para views e consultas analíticas.
- **Documentação**: início de cada notebook deve ter uma explicação do que o código vai fazer, listando as origens lidas e a tabela que será gerada.
- **Estilo**: seguir PEP 8; formatar com `black` e ordenar imports com `isort`. Lint com `ruff`.
- **Nomes de tabelas**: `snake_case`, prefixadas pela camada quando fora do Unity Catalog (`tb_bronze_`, `tb_silver_`, `tb_gold_`).
- **Nomes de colunas**: `snake_case`, sem acentos. Datas em UTC com sufixo `_ts` (timestamp) ou `_dt` (date).
- **Tabelas sempre em formato Delta.** Não usar Parquet cru para tabelas gerenciadas.
- **Idempotência**: transformações devem ser reexecutáveis sem duplicar dados (usar `MERGE` ou overwrite particionado).
- **Particionamento**: particionar Bronze/Silver por data de ingestão (`ingestion_dt`); evitar over-partitioning em Gold.
- **Segredos**: nunca hardcodar credenciais. Usar Databricks Secrets (`dbutils.secrets.get`) ou perfis da Databricks CLI.
- **Notebooks**: lógica reutilizável vai para módulos `.py` em `src/transformations/`; notebooks apenas orquestram e exploram.
- **Docstrings**: toda função de transformação deve documentar entrada, saída e granularidade da tabela resultante.

## Estrutura de Pastas

```
data-platform-360/
├── databricks.yml              # OBRIGATÓRIO na raiz — define o bundle, include e targets
├── resources/
│   ├── jobs/                   # definições de jobs (*.yml)
│   │   └── job_visao_360_daily.yml
│   └── pipelines/              # definições de pipelines ETL (*.yml) — opcional
├── src/
│   ├── transformations/        # lógica reutilizável (bronze/silver/gold)
│   ├── utils/
│   └── notebooks/              # notebooks de orquestração e exploração
├── tests/                      # testes unitários (pytest + chispa)
├── conf/                       # parâmetros de negócio que não cabem nos targets
├── .claude/skills/             # skills do AI Dev Kit
├── pyproject.toml              # dependências (uv)
└── README.md
```

## Comandos Comuns

```bash
# Ambiente local
uv sync                                # instalar dependências
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
./databricks-skills/install_skills.sh --install-to-genie  # subir skills para o workspace
```

## Regras de Comportamento

- **Sempre** invocar a skill do AI Dev Kit antes de escrever código Databricks (pipelines, jobs, dashboards, Unity Catalog).
- **Sempre** rodar `ruff` e `pytest` antes de propor um commit.
- **Sempre** validar schema da tabela de destino antes de escrever (evitar schema drift silencioso).
- **Nunca** usar Spark Declarative Pipelines (SDP / DLT) — proibido neste projeto.
- **Nunca** escrever em catálogo `prod` sem confirmação explícita do usuário.
- **Nunca** fazer `DROP` ou `OVERWRITE` em tabelas Gold sem backup ou confirmação.
- Preferir `MERGE INTO` para upserts em Silver/Gold em vez de delete + insert.
- Ao criar nova transformação, incluir teste unitário correspondente em `tests/`.
- Comentar a granularidade de toda tabela Gold criada (ex.: "1 linha por cliente").
- Usar um PAT com escopo limitado para o MCP server; não usar tokens com permissões amplas em dev.
- Preferir as skills e ferramentas do AI Dev Kit a gerar código de memória — elas refletem os padrões oficiais e reduzem alucinação.
