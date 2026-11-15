# MC714_Lab02

## Contrato gRPC

O arquivo [node.proto](node.proto) define a comunicação entre os nós do algoritmo
de eleição do Bully. Cada nó lógico executa um servidor gRPC e conhece os
endpoints dos demais nós por configuração. O contrato é independente da
linguagem de implementação.

### Serviço `Node`

| RPC | Finalidade | Resposta |
| --- | --- | --- |
| `Heartbeat(Ping)` | Verificar se um nó está ativo | `Pong` |
| `StartElection(ElectionMsg)` | Iniciar ou propagar uma eleição | `OkMsg` |
| `AnnounceCoordinator(CoordinatorMsg)` | Informar o novo coordenador | `AckMsg` |

Toda chamada gRPC deve configurar explicitamente um deadline ou timeout. Uma
chamada sem resposta dentro desse prazo é tratada pela aplicação como falha de
comunicação ou falha do nó, conforme o cenário do experimento.

### Regras acordadas do protocolo

1. Cada nó possui um `NODE_ID` inteiro único.
2. O nó com o maior ID entre os nós ativos tem prioridade e deve ser o
	coordenador.
3. Um nó que recebe `StartElection` de um nó com ID menor responde com
	`accepted = true` e inicia sua própria eleição, caso ainda não esteja
	participando de uma eleição mais nova.
4. Um nó que recebe uma eleição de um ID maior abandona a eleição local e
	aguarda o resultado da eleição superior.
5. Se nenhum nó com ID maior responder dentro do timeout da eleição, o nó que
	iniciou a eleição declara-se coordenador.
6. O coordenador informa sua vitória aos demais nós com
	`AnnounceCoordinator`.
7. O `election_id` identifica uma execução específica de eleição. Uma nova
	eleição deve usar um valor diferente.
	contador local combinado com o ID do nó.
8. Mensagens de uma eleição antiga não podem substituir o estado de uma
	eleição mais nova.
9. A ausência de resposta a `Heartbeat` dentro do timeout é registrada como
	falha detectada do coordenador. O nó então inicia uma nova eleição.
10. Os nós são processos independentes, mesmo quando executados como
	 contêineres na mesma máquina. O sistema deve ter pelo menos três nós
	 lógicos.

### Campos das mensagens

- `sender_id`: ID do nó que enviou a mensagem.
- `responder_id`: ID do nó que respondeu à chamada.
- `coordinator_id`: ID do coordenador anunciado.
- `election_id`: identificador da eleição associada à mensagem.
- `accepted`: indica se o nó aceitou ou processou a solicitação.
- `alive`: indica que o nó respondeu ao heartbeat.

### Geração dos stubs

Com o compilador Protocol Buffers e os plugins gRPC instalados, a validação do
contrato pode ser feita com:

```bash
protoc --descriptor_set_out=/tmp/mc714-node.pb node.proto
```

Para Python, instale as dependências:

```bash
python3 -m pip install grpcio grpcio-tools
```

Depois gere os arquivos de cliente e servidor:

```bash
python3 -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --grpc_python_out=. \
  node.proto
```

Isso cria `node_pb2.py` e `node_pb2_grpc.py`. Esses arquivos são gerados e não
devem ser editados manualmente.

## Execução com Docker Compose

O laboratório sobe três nós independentes. Cada serviço recebe seu identificador
por `NODE_ID` e escuta a porta `50051` dentro da rede Docker:

```bash
docker compose up --build
```

Para acompanhar apenas os logs dos nós:

```bash
docker compose logs -f
```

Os nós 1 e 2 enviam um heartbeat ao coordenador (nó 3) a cada três segundos.
Após duas falhas consecutivas, o nó inicia o algoritmo Bully: consulta os nós
com ID maior, declara-se coordenador se nenhum responder e anuncia o resultado
com `AnnounceCoordinator`. O nó 3, por ter o maior ID, vence enquanto estiver
ativo; se ele cair, o nó 2 vence entre os nós restantes.

Para parar e remover os contêineres e a rede:

```bash
docker compose down
```

## Testes e experimento

Os testes unitários são executados dentro da imagem, onde `grpcio` e os stubs
gerados estão disponíveis:

```bash
docker compose build
docker compose run --rm node1 pytest -q
```

O experimento reproduzível executa cenários normal e de falha, varia o intervalo
do heartbeat em 1, 2 e 3 segundos e repete cada configuração três vezes. Em
cada repetição, o nó 3 é derrubado e o tempo até o nó 2 assumir é medido:

```bash
python3 scripts/run_experiment.py
cat experiments/results.csv
cat experiments/summary.csv
```

`results.csv` contém uma linha por repetição. `summary.csv` contém, para cada
intervalo, as taxas de sucesso, média, desvio-padrão, p50 e p95 do tempo de
convergência. Para executar mais repetições, use, por exemplo,
`EXPERIMENT_REPETITIONS=5 python3 scripts/run_experiment.py`.
Os parâmetros também podem ser alterados por ambiente:

- `RPC_TIMEOUT_SECONDS`: deadline de cada chamada gRPC;
- `HEARTBEAT_INTERVAL_SECONDS`: intervalo entre heartbeats;
- `HEARTBEAT_FAILURE_THRESHOLD`: falhas consecutivas necessárias para iniciar
	uma eleição.

## Execução com Clusters de Tamanho Variável (Compose Dinâmico)

Para avaliar a escalabilidade do algoritmo Bully com qualquer número de nós (ex: $N=5, 7, 10$), utilize o script gerador:

```bash
# Gerar um compose com 5 nós (nó 5 como coordenador inicial e intervalo de 1.0s)
python3 scripts/generate_compose.py -n 5 -c 5 -i 1.0 -o compose.dynamic.yaml

# Iniciar o cluster de 5 nós
docker compose -f compose.dynamic.yaml up -d

# Visualizar logs
docker compose -f compose.dynamic.yaml logs -f

# Encerrar e limpar contêineres e volumes órfãos
docker compose -f compose.dynamic.yaml down --remove-orphans -t 1
```

## Bateria de Testes Automatizada e Métricas

Além do script de referência do enunciado (`scripts/run_experiment.py`), o projeto conta com uma bateria modular de testes que avalia múltiplos cenários com reset rápido dos contêineres e contagem de mensagens $\mathcal{O}(N^2)$:

```bash
# Executar bateria completa para N=3, 5, 7 e 10 nós (5 repetições para cada cenário)
python3 scripts/run_test_battery.py --scenario all -n 3 5 7 10 --repetitions 5

# Cenários disponíveis via --scenario:
#   baseline      : Falha e recuperação do coordenador
#   scalability   : Variação de N nós sob falha do coordenador
#   simultaneous  : Falha simultânea dos dois maiores nós
#   intermediate  : Falha de nó intermediário (sem afetar o líder)
#   all           : Executa todos os 4 cenários

# Gerar gráficos comparativos a partir dos resultados da bateria:
python3 scripts/plot_battery.py
```

Os dados brutos e sumarizados são salvos em `experiments/battery_results.csv` e `experiments/battery_summary.csv`. Os gráficos são gerados em `experiments/battery_message_scaling.png` e `experiments/battery_convergence_scaling.png`.

O modelo do sistema, as propriedades de correção, métricas e limitações estão
descritos em [REPORT.md](REPORT.md).