-- Exemplo de schema de banco de dados para análise de vendas
-- Este é um modelo de banco de dados dimensional para análise de dados de vendas

-- Tabela de dimensão: Produtos
CREATE TABLE dim_produtos (
    produto_id INT PRIMARY KEY,
    codigo_produto VARCHAR(20) NOT NULL,
    nome_produto VARCHAR(100) NOT NULL,
    descricao TEXT,
    categoria VARCHAR(50),
    subcategoria VARCHAR(50),
    marca VARCHAR(50),
    fornecedor VARCHAR(100),
    preco_unitario DECIMAL(10, 2),
    custo_unitario DECIMAL(10, 2),
    data_cadastro DATE,
    status VARCHAR(20) CHECK (status IN ('Ativo', 'Inativo', 'Descontinuado')),
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de dimensão: Clientes
CREATE TABLE dim_clientes (
    cliente_id INT PRIMARY KEY,
    codigo_cliente VARCHAR(20) NOT NULL,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    telefone VARCHAR(20),
    endereco VARCHAR(200),
    cidade VARCHAR(50),
    estado VARCHAR(2),
    cep VARCHAR(10),
    data_nascimento DATE,
    genero CHAR(1),
    segmento VARCHAR(50),
    data_cadastro DATE,
    status VARCHAR(20) CHECK (status IN ('Ativo', 'Inativo')),
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de dimensão: Lojas
CREATE TABLE dim_lojas (
    loja_id INT PRIMARY KEY,
    codigo_loja VARCHAR(20) NOT NULL,
    nome_loja VARCHAR(100) NOT NULL,
    tipo VARCHAR(50),
    endereco VARCHAR(200),
    cidade VARCHAR(50),
    estado VARCHAR(2),
    cep VARCHAR(10),
    regiao VARCHAR(50),
    gerente VARCHAR(100),
    telefone VARCHAR(20),
    email VARCHAR(100),
    data_inauguracao DATE,
    tamanho_m2 INT,
    status VARCHAR(20) CHECK (status IN ('Ativa', 'Inativa', 'Em reforma')),
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela de dimensão: Vendedores
CREATE TABLE dim_vendedores (
    vendedor_id INT PRIMARY KEY,
    codigo_vendedor VARCHAR(20) NOT NULL,
    nome VARCHAR(100) NOT NULL,
    email VARCHAR(100),
    telefone VARCHAR(20),
    cargo VARCHAR(50),
    departamento VARCHAR(50),
    loja_id INT,
    data_contratacao DATE,
    salario_base DECIMAL(10, 2),
    status VARCHAR(20) CHECK (status IN ('Ativo', 'Afastado', 'Desligado')),
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (loja_id) REFERENCES dim_lojas(loja_id)
);

-- Tabela de dimensão: Tempo (para facilitar análises temporais)
CREATE TABLE dim_tempo (
    data_id INT PRIMARY KEY,
    data_completa DATE UNIQUE NOT NULL,
    dia INT NOT NULL,
    dia_semana INT NOT NULL,
    dia_semana_nome VARCHAR(20) NOT NULL,
    semana_ano INT NOT NULL,
    mes INT NOT NULL,
    mes_nome VARCHAR(20) NOT NULL,
    trimestre INT NOT NULL,
    trimestre_nome VARCHAR(20) NOT NULL,
    semestre INT NOT NULL,
    ano INT NOT NULL,
    e_feriado BOOLEAN DEFAULT FALSE,
    nome_feriado VARCHAR(100),
    e_fim_semana BOOLEAN DEFAULT FALSE
);

-- Tabela de fatos: Vendas
CREATE TABLE fato_vendas (
    venda_id INT PRIMARY KEY,
    codigo_venda VARCHAR(20) NOT NULL,
    data_id INT NOT NULL,
    cliente_id INT NOT NULL,
    produto_id INT NOT NULL,
    vendedor_id INT NOT NULL,
    loja_id INT NOT NULL,
    quantidade INT NOT NULL,
    valor_unitario DECIMAL(10, 2) NOT NULL,
    valor_bruto DECIMAL(10, 2) NOT NULL,
    desconto DECIMAL(10, 2) DEFAULT 0,
    valor_liquido DECIMAL(10, 2) NOT NULL,
    custo_total DECIMAL(10, 2) NOT NULL,
    margem_lucro DECIMAL(10, 2) NOT NULL,
    forma_pagamento VARCHAR(50),
    parcelas INT DEFAULT 1,
    status_venda VARCHAR(20) CHECK (status_venda IN ('Concluída', 'Cancelada', 'Devolvida')),
    canal_venda VARCHAR(50),
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (data_id) REFERENCES dim_tempo(data_id),
    FOREIGN KEY (cliente_id) REFERENCES dim_clientes(cliente_id),
    FOREIGN KEY (produto_id) REFERENCES dim_produtos(produto_id),
    FOREIGN KEY (vendedor_id) REFERENCES dim_vendedores(vendedor_id),
    FOREIGN KEY (loja_id) REFERENCES dim_lojas(loja_id)
);

-- Índices para otimizar consultas
CREATE INDEX idx_fato_vendas_data ON fato_vendas(data_id);
CREATE INDEX idx_fato_vendas_cliente ON fato_vendas(cliente_id);
CREATE INDEX idx_fato_vendas_produto ON fato_vendas(produto_id);
CREATE INDEX idx_fato_vendas_vendedor ON fato_vendas(vendedor_id);
CREATE INDEX idx_fato_vendas_loja ON fato_vendas(loja_id);
CREATE INDEX idx_dim_produtos_categoria ON dim_produtos(categoria);
CREATE INDEX idx_dim_clientes_cidade ON dim_clientes(cidade, estado);
CREATE INDEX idx_dim_lojas_regiao ON dim_lojas(regiao);

-- Views para facilitar análises comuns

-- View de vendas por período
CREATE VIEW vw_vendas_por_periodo AS
SELECT 
    t.ano,
    t.mes,
    t.mes_nome,
    t.trimestre,
    SUM(f.valor_liquido) AS valor_total,
    SUM(f.margem_lucro) AS lucro_total,
    COUNT(DISTINCT f.venda_id) AS total_vendas,
    COUNT(DISTINCT f.cliente_id) AS total_clientes
FROM 
    fato_vendas f
    JOIN dim_tempo t ON f.data_id = t.data_id
WHERE 
    f.status_venda = 'Concluída'
GROUP BY 
    t.ano, t.mes, t.mes_nome, t.trimestre
ORDER BY 
    t.ano, t.mes;

-- View de vendas por categoria de produto
CREATE VIEW vw_vendas_por_categoria AS
SELECT 
    p.categoria,
    p.subcategoria,
    SUM(f.quantidade) AS quantidade_total,
    SUM(f.valor_liquido) AS valor_total,
    SUM(f.margem_lucro) AS lucro_total,
    AVG(f.valor_unitario) AS ticket_medio,
    COUNT(DISTINCT f.venda_id) AS total_vendas
FROM 
    fato_vendas f
    JOIN dim_produtos p ON f.produto_id = p.produto_id
WHERE 
    f.status_venda = 'Concluída'
GROUP BY 
    p.categoria, p.subcategoria
ORDER BY 
    valor_total DESC;

-- View de desempenho de vendedores
CREATE VIEW vw_desempenho_vendedores AS
SELECT 
    v.vendedor_id,
    v.nome AS nome_vendedor,
    l.nome_loja,
    SUM(f.valor_liquido) AS total_vendas,
    SUM(f.margem_lucro) AS total_lucro,
    COUNT(DISTINCT f.venda_id) AS num_vendas,
    AVG(f.valor_liquido) AS ticket_medio,
    SUM(f.margem_lucro) / SUM(f.valor_liquido) * 100 AS margem_percentual
FROM 
    fato_vendas f
    JOIN dim_vendedores v ON f.vendedor_id = v.vendedor_id
    JOIN dim_lojas l ON f.loja_id = l.loja_id
WHERE 
    f.status_venda = 'Concluída'
GROUP BY 
    v.vendedor_id, v.nome, l.nome_loja
ORDER BY 
    total_vendas DESC;

-- Exemplo de consulta para análise de vendas por região e período
-- SELECT 
--     l.regiao,
--     t.ano,
--     t.trimestre,
--     SUM(f.valor_liquido) AS valor_total,
--     COUNT(DISTINCT f.venda_id) AS num_vendas,
--     COUNT(DISTINCT f.cliente_id) AS num_clientes,
--     SUM(f.margem_lucro) AS lucro_total,
--     AVG(f.valor_liquido) AS ticket_medio
-- FROM 
--     fato_vendas f
--     JOIN dim_lojas l ON f.loja_id = l.loja_id
--     JOIN dim_tempo t ON f.data_id = t.data_id
-- WHERE 
--     f.status_venda = 'Concluída'
--     AND t.ano >= 2022
-- GROUP BY 
--     l.regiao, t.ano, t.trimestre
-- ORDER BY 
--     l.regiao, t.ano, t.trimestre; 