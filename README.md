# ETL com AWS, Terraform (IaC) e GitHub Actions (CI/CD)

Repositório referente à parte 4 do hands-on de ETL com AWS da fase de Big Data da Pós Tech de Machine Learning Engineering da FIAP.

O objetivo desse exercício é provisionar recursos de um pipeline de dados na AWS usando Terraform (Infraestrutura como Código - IaC) e configurar um pipeline de CI/CD com GitHub Actions.

## Descrição Geral

Este projeto implementa um pipeline ETL que:

- **Extrai** dados de uma fonte externa utilizando o pacote `ipeadatapy`.
- **Transforma** os dados aplicando diversas operações de transformação e agregação.
- **Carrega** os dados transformados em um bucket S3.
- Utiliza **AWS Glue** para executar os jobs ETL, incluindo extração, transformação e carregamento dos dados.
- Gerencia a infraestrutura utilizando **Terraform**, garantindo reprodutibilidade e controle de versão.

## Pré-requisitos

- **Conta AWS** configurada e com as permissões adequadas para provisionar os recursos.
- Credenciais AWS configuradas no secrets do repositório do GitHub.

## Estrutura dos Diretórios

- **`src/jobs/`**: Contém os scripts Python (`extract.py`, `transform.py`, `load.py`) utilizados para os jobs no AWS Glue.
  - `extract.py`: Extrai os dados da fonte.
  - `transform.py`: Aplica as transformações necessárias.
  - `load.py`: Carrega os dados transformados no bucket S3.
  
- **`src/tests/`**: Contém os testes unitários para validar as funções utilizadas nos scripts ETL.
  - `test_transform.py`: Testa as funções de transformação de dados do script `transform.py`.

- **`infra/`**: Contém os módulos Terraform responsáveis por provisionar a infraestrutura na AWS.

  - **`glue/`**: Configura o job do AWS Glue, incluindo o uso dos scripts de transformação.
  
  - **`terraform_backend/`**: Define os recursos de backend do Terraform, como buckets S3 para o estado do Terraform e tabelas DynamoDB para o lock de estado.
        
    - **`s3/`**: Configura o bucket S3 que armazenará o estado do Terraform. Este bucket é utilizado para garantir persistência e compartilhamento do estado entre diferentes ambientes e desenvolvedores.
        
    - **`dynamodb/`**: Provisiona a tabela DynamoDB usada para controle de lock do estado do Terraform, garantindo que apenas uma operação de `terraform apply` possa ocorrer de cada vez, prevenindo conflitos.
        
    - **`iam_role/`**: Define as permissões de IAM necessárias para permitir que o Terraform acesse e gerencie os recursos da AWS de forma segura.

## Pipeline CI/CD

O pipeline de CI/CD utiliza **GitHub Actions** para:

1. Verificar e aplicar boas práticas de código (Linting) com `flake8`.
2. Rodar os testes unitários para validar as transformações de dados.
3. Subir os scripts de transformação para o bucket S3.
4. Provisionar a infraestrutura via Terraform.

Para disparar o pipeline, basta fazer um `push` para o branch `main`.

Se o arquivo `terraform_action.txt` conter `apply` irá provsionar os recursos, se conter `destroy`, irá destruir os recursos.