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
	eleição deve usar um valor diferente; recomenda-se gerar o valor com um
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