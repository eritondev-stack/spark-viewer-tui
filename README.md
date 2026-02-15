# Projeto Textual

Aplicação simples usando [Textual](https://textual.textual.io/) com sidebar, textarea e tabela.

## Requisitos

- Python 3.12+
- [uv](https://github.com/astral-sh/uv)

## Como Executar

```bash
uv run python app.py
```

## Layout

A aplicação consiste em:

- **Sidebar** (esquerda): Barra lateral com título e informações
- **TextArea** (direita, acima): Campo de texto para digitação
- **Table** (direita, abaixo): Tabela com dados de exemplo

## Como Sair

Pressione `Ctrl+C` ou `Ctrl+D` para sair da aplicação.

## Estrutura do Projeto

```
projeto-textual/
├── pyproject.toml    # Configuração do projeto
├── README.md         # Esta documentação
└── app.py            # Aplicação principal
```
