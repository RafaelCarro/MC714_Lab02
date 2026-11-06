# Trabalho 2: Eleicao Bully e Detector de Falhas

## Objetivo

O projeto implementa o algoritmo de eleicao de lider Bully e um detector de
falhas baseado em heartbeat. Cada no e um processo independente executado em um
container Docker e se comunica somente por gRPC na rede do Compose.

## Modelo do sistema

- **Tempo:** parcialmente sincrono. Existe um limite conhecido para cada RPC,
  mas o escalonamento dos processos e a entrega da rede podem atrasar.
- **Canais:** TCP/gRPC, ordenados por conexao, sem duplicacao intencional e sem
  garantia de entrega quando um processo esta indisponivel. Cada chamada usa
  deadline explicito.
- **Falhas:** crash-stop durante o experimento. O no que falha deixa de
  responder; particoes e falhas bizantinas nao sao modeladas.
- **Reinicio:** um reinicio inicia um novo processo com o estado inicial
  configurado. Nao ha persistencia de eleicoes entre reinicios.
- **Topologia:** tres nos logicos, com IDs 1, 2 e 3. O maior ID ativo possui
  prioridade.

## Algoritmos

### Detector de falhas

Os nos que nao sao o coordenador enviam `Heartbeat(Ping)` a cada intervalo
configuravel. O servidor responde `Pong(alive=true)`. Duas falhas consecutivas
por timeout iniciam uma eleicao. Um `Pong` valido zera o contador de falhas.

### Bully

O no que detecta a falha envia `StartElection` aos nos de ID maior. Um no maior
responde `accepted=true` e inicia ou propaga a mesma eleicao. Se nenhum no maior
responder, o iniciador declara-se coordenador e envia `AnnounceCoordinator` a
todos os nos. Anuncios com `election_id` antigo sao rejeitados.

## Propriedades de correcao

- **Seguranca:** em uma execucao sem mensagens antigas atrasadas e com um unico
  conjunto conectado de nos ativos, somente o maior ID ativo se declara
  coordenador.
- **Vivacidade:** depois de uma falha permanente do coordenador, se os nos
  restantes continuarem conectados e respondendo, uma nova eleicao termina e o
  maior ID restante e anunciado.
- **Deteccao:** uma falha e declarada depois de pelo menos dois deadlines
  consecutivos, evitando que um atraso isolado provoque eleicao.

Essas propriedades sao verificadas pelos testes em `tests/test_server.py` e pelo
experimento de falha em `scripts/run_experiment.py`.

## Experimento reproduzivel

O script varia o intervalo do heartbeat em tres niveis: 1, 2 e 3 segundos. Para
cada configuracao executa repeticoes independentes (o script usa tres por
padrao; a coleta versionada abaixo usou duas). Em cada repeticao,
primeiro verifica o funcionamento normal dos heartbeats, depois para o no 3,
mede o tempo ate o no 2 ser anunciado como coordenador e salva os dados brutos
em `experiments/results.csv`.

Metricas:

- vencedor apos a falha;
- taxa de sucesso no cenario normal;
- taxa de sucesso no cenario de falha;
- tempo de convergencia em segundos;
- intervalo do heartbeat;
- timeout RPC e limiar de falhas.

Para cada intervalo, `experiments/summary.csv` calcula media, desvio-padrao,
p50 e p95 do tempo de convergencia. O p95 e incluido por se tratar de uma
metrica de latencia, conforme exigido pelo enunciado.

Resultados da coleta versionada com duas repeticoes por configuracao:

| Intervalo heartbeat (s) | Repeticoes | Sucesso normal | Sucesso falha | Media (s) | Desvio-padrao (s) | p50 (s) | p95 (s) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 1.0 | 1.0 | 3.049 | 0.001 | 3.049 | 3.050 |
| 2 | 2 | 1.0 | 1.0 | 5.300 | 0.042 | 5.300 | 5.326 |
| 3 | 2 | 1.0 | 1.0 | 5.311 | 0.086 | 5.311 | 5.366 |

Os dados brutos correspondentes estao em `experiments/results.csv`. O tempo
medido inclui a deteccao por heartbeat, a eleicao e a propagacao do anuncio.

Comandos:

```bash
python3 scripts/run_experiment.py
cat experiments/results.csv
cat experiments/summary.csv
```

## Limitacoes

Os tres containers compartilham a mesma maquina fisica e, portanto, nao
representam dominios de falha independentes. O detector usa timeouts fixos e
pode produzir falso positivo em atrasos maiores que o deadline. Nao sao
modeladas falhas bizantinas, recuperacao com preservacao de estado ou particoes
que mantenham ambos os lados ativos.
