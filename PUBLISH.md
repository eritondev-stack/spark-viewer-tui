# Publicar no PyPI

## 1. Criar conta no PyPI

- Acesse https://pypi.org/account/register/
- Confirme o email

## 2. Criar API Token

- Acesse https://pypi.org/manage/account/token/
- Scope: **Entire account** (primeira vez)
- Copie o token gerado (`pypi-xxxx...`), ele aparece uma vez so

## 3. Instalar ferramentas

```bash
uv pip install build twine
```

## 4. Build do pacote

```bash
python -m build
```

Vai gerar dois arquivos em `dist/`:
```
dist/
  spark_viewer_tui-0.1.0-py3-none-any.whl
  spark_viewer_tui-0.1.0.tar.gz
```

## 5. Publicar no PyPI

```bash
twine upload dist/*
```

Quando pedir:
- **Username:** `__token__`
- **Password:** o token `pypi-xxxx...`

## 6. Verificar

Acesse https://pypi.org/project/spark-viewer-tui/

## 7. Instalar em outra maquina

```bash
pip install spark-viewer-tui
```

Rodar:
```bash
spark-viewer
```

## Publicar nova versao

1. Altere a `version` no `pyproject.toml` (ex: `0.2.0`)
2. Delete a pasta `dist/` antiga: `rm -rf dist/`
3. Repita os passos 4 e 5

## Testar antes no TestPyPI (opcional)

Se quiser testar antes de publicar no PyPI real:

```bash
# Upload para TestPyPI
twine upload --repository testpypi dist/*

# Instalar do TestPyPI
pip install --index-url https://test.pypi.org/simple/ spark-viewer-tui
```

Criar token do TestPyPI em https://test.pypi.org/manage/account/token/
