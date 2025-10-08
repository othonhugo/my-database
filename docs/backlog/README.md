# Project Backlog: `my-database`

Este backlog é dividido em fases sequenciais. Cada fase representa um marco de desenvolvimento com um objetivo claro e constrói sobre o trabalho da fase anterior.

## Fases do Desenvolvimento

### [Fase 1: Motor de Armazenamento (Log Append-Only)](01.md)

Onde construímos a **fundação** do nosso sistema: um motor de armazenamento simples, durável e correto. A prioridade aqui é garantir que os dados escritos jamais sejam perdidos, mesmo que a leitura seja lenta.

### [Fase 2: Indexação em Memória (Aceleração de Leituras)](02.md)

Onde tornamos nosso banco de dados **rápido**. Implementamos um índice em memória para eliminar a varredura completa do disco, transformando a complexidade de leitura de O(N) para O(1) e tornando o sistema verdadeiramente performático.

### [Fase 3: Gerenciamento de Log e Compactação](03.md)

Onde tornamos nosso banco de dados **sustentável**. Introduzimos a segmentação de logs e um processo de compactação para recuperar espaço em disco e garantir que o tempo de inicialização permaneça sob controle à medida que os dados crescem.

### [Fase 4: Camada de Rede, Concorrência e Servidor](04.md)

Onde transformamos nossa biblioteca em um **serviço**. Construímos uma camada de rede para que clientes possam se conectar e interagir com o banco de dados, garantindo que ele opere de forma segura sob acesso concorrente.

### [Fase 5: Usabilidade e Documentação](05.md)

Onde **polimos** o projeto. Criamos ferramentas para facilitar o uso (como um CLI) e finalizamos a documentação para que outros possam entender, usar e contribuir com o nosso trabalho.
