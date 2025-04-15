# Sistema de Coleta e Armazenamento de Dados do IBOVESPA

Este projeto fornece uma solução simplificada para coletar dados históricos do IBOVESPA e armazená-los em uma tabela MySQL, facilitando a atualização diária para dashboards e análises.

## Características

- Coleta automática de dados do IBOVESPA via Yahoo Finance
- Armazenamento em uma única tabela SQL otimizada (`Ft_Ibovespa`)
- Atualização inteligente (apenas dados mais recentes)
- Sistema de logs detalhados
- Interface de linha de comando simples
- Suporte para agendamento automatizado

## Estrutura

```
.
├── orquestrador.py       # Script principal de orquestração
├── fetch_data.py         # Módulo para obtenção de dados do IBOVESPA
├── db_manager.py         # Módulo para gerenciamento do banco MySQL
├── scheduler.py          # Script para agendamento de atualizações
├── .env                  # Arquivo de configuração com credenciais
├── .env.example          # Exemplo de configuração
├── README.md             # Esta documentação
├── logs/                 # Diretório para logs
└── data/                 # Diretório opcional para armazenamento de CSVs
```

## Requisitos

- Python 3.7+
- MySQL 5.7+ ou MariaDB 10.3+
- Pacotes Python: yfinance, pandas, mysql-connector-python, python-dotenv

## Instalação

1. Clone o repositório
2. Crie um ambiente virtual (recomendado)

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

3. Instale as dependências

```bash
pip install -r requirements.txt
```

4. Configure o arquivo .env com suas credenciais

```bash
cp .env.example .env
# Edite o arquivo .env com seu editor preferido
```

## Uso

### Configuração inicial

```bash
# Cria a tabela Ft_Ibovespa se não existir
python orquestrador.py setup
```

### Carga inicial de dados históricos

```bash
# Carrega dados desde 01/01/2021
python orquestrador.py loaddata

# Ou especifique uma data inicial
python orquestrador.py loaddata --start-date "2022-01-01"
```

### Atualização com dados recentes

```bash
# Atualiza com os dados mais recentes
python orquestrador.py update
```

### Sincronização automática

```bash
# Detecta automaticamente se a tabela está vazia e realiza a ação apropriada
# (carga inicial ou atualização)
python orquestrador.py sync
```

## Estrutura da tabela

A tabela `Ft_Ibovespa` contém os seguintes campos:

| Campo | Tipo | Descrição |
|-------|------|-----------|
| id | INT | Chave primária |
| date | DATE | Data do registro |
| open | DECIMAL(12,2) | Preço de abertura |
| high | DECIMAL(12,2) | Preço máximo |
| low | DECIMAL(12,2) | Preço mínimo |
| close | DECIMAL(12,2) | Preço de fechamento |
| volume | BIGINT | Volume negociado |
| year | INT | Ano |
| month | INT | Mês |
| day | INT | Dia |
| rentabilidade | DECIMAL(10,4) | Rentabilidade diária (%) |
| ma_7 | DECIMAL(12,2) | Média móvel de 7 dias |
| media_movel_7d | DECIMAL(12,2) | Média móvel de 14 dias |
| media_movel_14d | DECIMAL(12,2) | Média móvel de 21 dias |
| media_movel_50d | DECIMAL(12,2) | Média móvel de 50 dias |
| media_movel_200d | DECIMAL(12,2) | Média móvel de 200 dias |
| created_at | TIMESTAMP | Data de criação do registro |
| updated_at | TIMESTAMP | Data de atualização do registro |

## Agendamento

Para configurar atualizações diárias automáticas:

### Linux/Mac (crontab)

```bash
# Adicione ao crontab para executar todos os dias às 19:00
0 19 * * * /caminho/para/python /caminho/para/scheduler.py >> /caminho/para/logs/cron.log 2>&1
```

### Windows (Agendador de Tarefas)

1. Crie um arquivo .bat com o seguinte conteúdo:

```batch
@echo off
C:\caminho\para\python.exe C:\caminho\para\scheduler.py
```

2. Configure o Agendador de Tarefas do Windows para executar este arquivo diariamente após o fechamento do mercado.

## Uso para Dashboards

Os dados armazenados na tabela `Ft_Ibovespa` são ideais para alimentar dashboards, pois:

1. As médias móveis já estão pré-calculadas
2. Campos de data estão decompostos para facilitar análises por período
3. Rentabilidade diária está calculada e pronta para uso
4. Atualizações são incrementais e não duplicam dados

## Solução de Problemas

- **Erro de conexão com o banco**: Verifique as credenciais no arquivo .env
- **Erro ao criar tabela**: Verifique se o usuário tem permissões para criar tabelas
- **Erro "window"**: Este erro já foi corrigido. A palavra "window" é reservada no MySQL e agora é tratada corretamente
- **Problema de codificação**: Todos os arquivos de log usam codificação UTF-8 para compatibilidade

## Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.
