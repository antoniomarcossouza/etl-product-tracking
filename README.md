## Sobre os dados

Os arquivos contêm informações sobre códigos e eventos de rastreamento de compras em plataformas de e-commerce.

Cada arquivo representa um batch de transações a serem executadas em um banco de dados, onde cada linha é uma transação diferente, identificada pela coluna "Op". Essas transações podem incluir operações de INSERT (identificada pela letra I) ou UPDATE (identificada pela letra U).

## Requisitos
- [x] Elaborar um processo de ETL que realize extração, transformação e carregamento dos dados presentes nos arquivos fornecidos, conforme as transações especificadas em cada linha.
- [x] Realizar a normalização e modelagem do banco de dados, bem como suas tabelas, conforme for apropriado.
- [ ] O processo de ETL deve ser desenvolvido para ser executado utilizando o Docker, garantindo que todas as etapas sejam executadas dentro de containers.
- [ ] Em um cenário real, os arquivos seriam disponibilizados à medida em que são criados. Portanto, é **obrigatório** que os arquivos sejam extraídos, transformados e carregados para o banco de dados no máximo de 5 em 5 por vez, isto é, o processamento direto de todos os dados não é permitido.
- [ ] Documentar o passo a passo de como executar a pipeline, bem como pontos importantes do desenvolvimento, funcionalidades e decisões de implementação.
- [x] O Amazon Redshift (Data Warehouse) possui uma interface similar ao PostgreSQL mas não implementa alguns comandos e funcionalidades. Um destes comandos é `INSERT ... ON CONFLICT`. Desenvolva a lógica de inserção com update (upsert) dos dados sem a utilização deste comando nativo do PostgreSQL.
- [x] Desenvolver 3 queries como forma de relatório para validação dos dados:
	- [x] Total de rastreamentos criados por minuto
	- [x] Total de eventos por código de rastreamento
	- [x] TOP 10 descrições de eventos mais comuns e seus totais

## Dicionário de dados
| Coluna               | Descrição                                           |
| -------------------- | --------------------------------------------------- |
| Op                   | tipo de operação da transação                       |
| oid__id              | identificador do rastreamento                       |
| createdAt            | data de criação do rastreamento                     |
| updatedAt            | data de atualização do rastreamento                 |
| lastSyncTracker      | data da última sincronização do rastreamento        |
| array_trackingEvents | contém informações sobre os eventos de rastreamento |
| createdAt            | data de criação do evento                           |
| trackingCode         | código de rastreamento                              |
| status               | status do evento                                    |
| description          | descrição do evento                                 |
| trackerType          | código da transportadora                            |
| from                 | agência de partida do evento                        |
| to                   | agência de destino do evento                        |