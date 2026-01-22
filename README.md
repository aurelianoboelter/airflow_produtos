# Implementação Modelo Pre-Commit

**Adotando um modelo otimizado e profissional de configuração para Ruff + Black + Airflow + pre-commit, balanceando padronização e desempenho (usado em pipelines CI/CD de projetos reais de dados e airflow):**

1. Criar o Ficheiro de Configuração:
 Criar o ficheiro .pre-commit-config.yaml na raiz do projeto com o seguinte conteúdo.


2. Instale os Hooks de Pré-commit:
O projeto utiliza [pre-commit hooks](https://pre-commit.com/) para garantir a qualidade e a padronização do código antes de cada commit. Para ativá-los, execute

```bash
pre-commit install
```

2. Instalando as dependencias (black ):

```bash
pip install black
```
•	Comando de teste black padrão

```bash
pre-commit run black --all-files
```

3. Instalando as dependencias (isort):

```bash
pip install isort
```
•	Comando de teste isort padrão

```bash
pre-commit run isort --all-files
```

4. Instalando as dependencias (ruff):

```bash
pip install ruff
```
•	Comando de teste ruff padrão

```bash
pre-commit run ruff --all-files
```


5. Realizar teste manual ou iniciando commit Git:

```bash
pre-commit run --all-files
```

Adiciona novos arquivos e modificados, funciona apenas dentro da pasta onde comando é executado. Não registra deletados

```bash
git add .
```
Adiciona novos arquivos, modificados e deletados em todo repositório

```bash
git add -A
```

```bash
git commit -m "seu texto"  -
```

**Instalação e configuração detect-secrets**

5. instalar dependencias

```bash
pip install detect-secrets
```

6. incluir no arquivo .pre-commit-config.yaml

```bash
repos:
  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.5.0    # ou outra tag estável; depois você pode rodar `pre-commit autoupdate`
    hooks:
      - id: detect-secrets
        args:
          - --baseline
          - .secrets.baseline
```

7. Gerar a baseline inicial

```bash
detect-secrets scan > .secrets.baseline
```

8. Testando "secret" do hook

```bash
git add .
```

```bash
git add commit -m "prefixo: texto"

```
