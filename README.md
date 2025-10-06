# my-database

![Status](https://img.shields.io/badge/status-em%20desenvolvimento-yellow)
![License](https://img.shields.io/badge/license-MIT-blue)

## Sobre o Projeto

**my-database** é um projeto focado no aprendizado dos conceitos internos e fundamentais de um sistema de banco de dados. O objetivo não é criar um produto comercial, mas sim desmistificar o que acontece "por baixo dos panos" em sistemas como PostgreSQL, Redis ou MongoDB, construindo uma versão simplificada do zero.

Este projeto é destinado a estudantes e entusiastas de software que desejam aprofundar seus conhecimentos em estruturas de dados, algoritmos, operações de I/O e arquitetura de sistemas.

## Arquitetura Geral

Nossa abordagem será incremental, começando com o sistema mais simples possível e adicionando camadas de complexidade e otimização.

### 1. Motor de Armazenamento (Append-Only Log)

O coração do nosso banco de dados será um arquivo de log. Toda operação de escrita (`SET`, `DELETE`) será simplesmente anexada ao final deste arquivo.

-   **Prós:** Design extremamente simples e robusto. Garante **durabilidade**, pois os dados são escritos em disco. O log é um registro imutável de todas as transações, o que facilita a recuperação e a depuração.
-   **Desafio:** As leituras podem se tornar lentas à medida que o arquivo cresce, pois, inicialmente, precisaremos escanear o arquivo inteiro para encontrar o valor mais recente de uma chave.

### 2. Otimização de Leitura (Hash Index)

Para resolver o problema da leitura lenta, construiremos um índice em memória.

-   **Como funciona:** Ao iniciar, o banco de dados lê todo o arquivo de log e armazena em um Hash Map um mapeamento de cada `chave` para sua `posição (offset)` no arquivo de disco.
-   **Resultado:** As operações de leitura (`GET`) se tornam quase instantâneas (complexidade O(1)), pois podemos ir diretamente para a posição correta no arquivo sem precisar escaneá-lo.

## Roteiro do Projeto (Roadmap)

Dividimos o desenvolvimento em fases claras e alcançáveis.

-   [x] **Fase 1: Motor de Armazenamento**
    -   [x] Implementar a capacidade de anexar operações `SET` a um arquivo `database.log`.
    -   [x] Implementar a leitura `GET` por meio de escaneamento completo do arquivo.
    -   [x] Implementar a exclusão `DELETE` (anexando um marcador de exclusão).

-   [ ] **Fase 2: Indexação para Leituras Rápidas**
    -   [ ] Construir o índice Hash em memória durante a inicialização.
    -   [ ] Modificar a operação `GET` para usar o índice.
    -   [ ] Atualizar o índice em memória a cada operação `SET`.

-   [ ] **Fase 3: Interface e Rede**
    -   [ ] Criar um REPL (Read-Eval-Print Loop) para interagir com o banco de dados via terminal.
    -   [ ] Implementar um parser de comandos simples (`SET key value`, `GET key`).
    -   [ ] (Opcional) Expor o banco de dados através de um servidor TCP simples.

## Como Contribuir

Este é um projeto de aprendizado, e a colaboração é a parte mais importante. Para mantermos o projeto organizado e garantirmos que todos possam contribuir de forma eficaz, seguimos um [fluxo de trabalho específico](https://github.com/sourceunion/.github/blob/main/CONTRIBUTING.md).

## Licença

Este projeto está licenciado sob a [Licença MIT](https://github.com/sourceunion/.github/blob/main/LICENSE).