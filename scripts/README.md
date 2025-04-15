# Scripts Python

Esta pasta contém scripts Python para processamento, limpeza e transformação de dados.

## Scripts Disponíveis

### Processamento de Dados
- **data_cleaning.py**: Script completo para limpeza de dados com tratamento de valores ausentes, remoção de duplicatas e normalização de features
- **data_transformation.py**: Transformação de variáveis categóricas em dummies e conversão de tipos de dados

### Integração de APIs
- **api_connector.py**: Utilitários para conexão com APIs REST e transformação dos dados em DataFrames pandas
- **json_processor.py**: Funções para processar e normalizar dados em formato JSON

### Visualização
- **visualization_utils.py**: Funções reutilizáveis para criação de visualizações comuns (barras, linhas, dispersão, etc.)
- **plot_templates.py**: Templates de estilos para gráficos consistentes em todos os projetos

## Organização

- Separar scripts por funcionalidade ou projeto
- Utilizar nomes descritivos para os arquivos
- Incluir comentários e documentação clara no código

## Módulos Recomendados

- **Pandas**: Manipulação e análise de dados
- **NumPy**: Computação numérica
- **SQLAlchemy**: Interação com bancos de dados
- **Matplotlib/Seaborn**: Visualização de dados
- **Scikit-learn**: Aprendizado de máquina

## Boas Práticas

- Criar funções reutilizáveis
- Documentar parâmetros e valores de retorno
- Incluir tratamento de erros
- Considerar criar um ambiente virtual com requirements.txt

## Exemplo de Estrutura

```
scripts/
  ├── data_cleaning/
  │   ├── remove_duplicates.py
  │   └── handle_missing_values.py
  ├── data_transformation/
  │   ├── normalize_features.py
  │   └── create_features.py
  └── utils/
      ├── database_connection.py
      └── file_handlers.py
``` 