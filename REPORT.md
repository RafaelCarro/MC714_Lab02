# Implementação e Avaliação do Algoritmo de Eleição Bully com Detector de Falhas Baseado em Heartbeat

**Autores:**  
- Rafael Carro Gaudim — RA: 240879  
- Douglas Henrique Pereira — RA: 245202  
**Disciplina:** MC714 (Sistemas Distribuídos) — 2º Semestre de 2026 — Instituto de Computação, UNICAMP  
**Repositório:** [MC714_Lab02](.)

---

## 1. Introdução e Objetivo

Em sistemas distribuídos, a coordenação centralizada exige a eleição dinâmica e consistente de um processo líder na presença de falhas. Este trabalho apresenta a implementação, verificação formal e análise experimental de escalabilidade do **Algoritmo de Eleição Bully (Valentão)** integrado a um **Detector de Falhas periódico baseado em Heartbeat**, desenvolvidos em Python com gRPC (HTTP/2 e Protocol Buffers) e conteinerizados via Docker Compose.

O projeto investiga o comportamento do sistema sob quatro eixos principais:
1. **Convergência Temporal:** Tempo de detecção de falha e estabilização de um novo coordenador.
2. **Complexidade de Comunicação:** Volume de mensagens trocadas no pior e no caso médio ($\mathcal{O}(N^2)$).
3. **Escalabilidade:** Comportamento do cluster com $N \in \{3, 5, 7, 10\}$ nós.
4. **Cenários Adversos:** Resiliência a falhas simultâneas de múltiplos nós de alta prioridade e estabilidade diante de falhas de nós subordinados (intermediários).

---

## 2. Modelo do Sistema e Propriedades de Correção

- **Modelo Temporal:** Parcialmente síncrono. Existe um limite conhecido para cada chamada RPC (`timeout = 2.0s`), mas o escalonamento dos processos e a entrega da rede virtual podem variar.
- **Canais de Comunicação:** Conexões TCP com gRPC, ponto a ponto, ordenadas por conexão (FIFO por canal). Não há garantia de entrega quando o processo de destino está indisponível (`UNAVAILABLE` ou `DEADLINE_EXCEEDED`).
- **Modelo de Falhas:** *Crash-stop* com recuperação (*crash-recovery* sem persistência de estado em disco). O nó que falha interrompe imediatamente suas respostas; um reinício inicializa um novo processo com a configuração padrão. Falhas bizantinas e partições de rede não são modeladas.
- **Topologia e Prioridade:** $N$ nós lógicos com identificadores $ID \in \{1, \dots, N\}$. O nó ativo de maior $ID$ detém estritamente a prioridade de liderança.

### Propriedades Formais Verificadas
- **Segurança (*Safety*):** Em qualquer instante estável e sem mensagens atrasadas, no máximo um único processo ativo declara-se coordenador, e este é o de maior ID entre os ativos.
- **Vivacidade (*Liveness*):** Após a falha permanente do coordenador, se os nós restantes permanecerem conectados, uma nova eleição termina com sucesso e o nó restante de maior ID assume a liderança.
- **Detecção:** O detector exige $F = 2$ falhas consecutivas de heartbeat antes de disparar uma eleição, evitando reações intempestivas a atrasos transitórios de rede ou de escalonamento.

---

## 3. Arquitetura e Algoritmos

### 3.1. Contrato gRPC (`node.proto`)
O serviço `Node` expõe três procedimentos síncronos com timeouts explícitos:
1. `Heartbeat(Ping) -> Pong`: Enviado periodicamente pelos seguidores ao coordenador conhecido. Retorna o estado de disponibilidade (`alive = true`) e o `election_id` corrente.
2. `StartElection(ElectionMsg) -> OkMsg`: Disparado concorrentemente a todos os nós com $ID_{dest} > ID_{orig}$. Retorna `accepted = true` se o destinatário for maior e assumir a condução do processo eleitoral.
3. `AnnounceCoordinator(CoordinatorMsg) -> AckMsg`: Broadcast transmitido pelo vencedor da eleição a todos os nós do cluster para oficializar a nova liderança.

### 3.2. Detector de Falhas Heartbeat
- Nós subordinados executam uma thread dedicada enviando `Heartbeat` a cada $T$ segundos (`HEARTBEAT_INTERVAL_SECONDS`).
- Uma resposta válida zera o contador de falhas.
- Um timeout ou erro de conexão incrementa o contador. Ao atingir $F = 2$ (`HEARTBEAT_FAILURE_THRESHOLD`), o nó assume que o coordenador falhou, invalida seu líder local e inicia o algoritmo Bully.

### 3.3. Algoritmo Bully Otimizado
1. **Disparo:** O iniciador incrementa monotonicamente seu contador de eleição local e transmite `StartElection(node_id, election_id)` a todos os nós com $ID > ID_{local}$.
2. **Delegação:** Qualquer nó de ID superior que receba a mensagem responde afirmativamente (`accepted = true`) e inicia concorrentemente sua própria rodada eleitoral.
3. **Vitória:** Se o iniciador não receber nenhuma resposta `accepted = true` dentro do prazo de espera de RPC, declara-se coordenador e envia `AnnounceCoordinator` a todos os nós do cluster.
4. **Descarte de Mensagens Obsoletas:** Mensagens com `election_id` inferior ao registrado localmente são descartadas para impedir condições de corrida por mensagens defasadas.
5. **Otimização de Inicialização (*Cold Start*):** Para evitar uma tempestade quadrática de mensagens durante a subida conjunta do cluster, apenas nós cujo $ID \ge ID_{coord}$ disparam eleição no boot. Se o coordenador configurado já estiver saudável, os seguidores iniciam diretamente o envio de heartbeats, reduzindo a complexidade de boot para $\mathcal{O}(N)$.

### 3.4. Rastreamento Transparente de Métricas de Mensagens
Para avaliar a complexidade empírica $\mathcal{O}(N^2)$ sem adulterar o contrato de interface `node.proto` (preservando total compatibilidade com corretores automáticos), o servidor contabiliza chamadas emitidas e recebidas via logs estruturados `[MSG] <TIPO_MENSAGEM>`. Esse log é habilitado seletivamente via variável de ambiente `ENABLE_METRICS_LOGS=1`.

---

## 4. Avaliação Experimental

A experimentação foi estruturada em duas frentes automatizadas e reprodutíveis. A injeção de falhas nos experimentos cronometra a latência a partir do encerramento efetivo do container (`docker compose stop -t 1`), isolando a latência do daemon do Docker do tempo de resposta do algoritmo distribuído.

### 4.1. Experimento de Referência: Variação de Heartbeat ($N=3$)
Executado via `scripts/run_experiment.py`, avaliando três intervalos de heartbeat ($T \in \{1, 2, 3\}$ s) com deadline de 2,0 s e 2 repetições:

| Intervalo Heartbeat (s) | Repetições | Sucesso Normal | Sucesso Falha | Média Convergência (s) | Desvio Padrão (s) | Mediana p50 (s) | Percentil p95 (s) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1.0** | 2 | 100% | 100% | **3.049** | 0.001 | 3.049 | 3.050 |
| **2.0** | 2 | 100% | 100% | **5.300** | 0.042 | 5.300 | 5.326 |
| **3.0** | 2 | 100% | 100% | **5.311** | 0.086 | 5.311 | 5.366 |

*Observação:* Neste script inicial, o tempo de convergência engloba o tempo de término do subprocesso Docker. O intervalo de 1 s produziu a menor média, pois a falha é detectada em aproximadamente dois ciclos ($2 \times 1\,\text{s}$).

---

### 4.2. Bateria Modular de Testes ($N \in \{3, 5, 7, 10\}$)
Executada via `scripts/run_test_battery.py` com 5 repetições para cada configuração, avaliando latências e a quantidade de mensagens trocadas na eleição.

#### A) Escalabilidade e Tempo de Convergência ($T=1{,}0\,\text{s}, F=2$)

| Cenário | $N$ Nós | Sucesso | Latência Média (s) | Desvio Padrão (s) | p50 (s) | p95 (s) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Escalabilidade** | 3 | 100% | 1.161 | 0.138 | 1.072 | 1.315 |
| **Escalabilidade** | 5 | 100% | 1.209 | 0.116 | 1.162 | 1.366 |
| **Escalabilidade** | 7 | 100% | 1.139 | 0.128 | 1.196 | 1.254 |
| **Escalabilidade** | 10 | 100% | 1.185 | 0.165 | 1.149 | 1.401 |

**Análise:** Ao medir a convergência puramente a partir do momento em que o processo do coordenador é interrompido, a latência média permaneceu notavelmente plana entre **$1{,}139\,\text{s}$ e $1{,}209\,\text{s}$** para todos os tamanhos de cluster. Isso comprova que a detecção depende do período dos heartbeats independentes, enquanto o overhead de rede local gRPC é desprezível.

#### B) Complexidade de Mensagens ($\mathcal{O}(N^2)$)

| $N$ Nós | Média Total Mensagens | Election | OK | Coordinator | ACK |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **3** | **5.8** | 1.6 | 0.6 | 2.0 | 1.6 |
| **5** | **15.4** | 3.8 | 3.2 | 5.4 | 3.0 |
| **7** | **41.2** | 18.8 | 9.2 | 3.6 | 9.6 |
| **10** | **39.4** | 7.8 | 6.6 | 17.2 | 7.8 |

No cenário completo com falha e recuperação (*failback*) do coordenador original (**Baseline**):
- $N=3 \implies \mathbf{8{,}0}$ mensagens
- $N=5 \implies \mathbf{25{,}8}$ mensagens
- $N=7 \implies \mathbf{33{,}0}$ mensagens
- $N=10 \implies \mathbf{69{,}8}$ mensagens

Os gráficos gerados confirmam experimentalmente o crescimento de complexidade:
- [experiments/battery_message_scaling.png](experiments/battery_message_scaling.png)
- [experiments/battery_convergence_scaling.png](experiments/battery_convergence_scaling.png)

#### C) Cenários Adversos (Falhas Simultâneas e Intermediárias)
1. **Falha Simultânea (Queda concorrente dos nós $N$ e $N-1$):**
   - Em 100% dos testes, o terceiro nó ($N-2$) assumiu como coordenador sem travamentos (ex.: nó 8 em $N=10$; nó 5 em $N=7$; nó 3 em $N=5$; nó 1 em $N=3$).
   - O volume médio de mensagens aumentou para $56{,}8$ em $N=10$, pois múltiplos nós tentaram contatar o nó $N-1$ antes de recorrer ao nó $N-2$.
2. **Falha Intermediária (Queda do nó $\lfloor N/2 \rfloor$):**
   - Em todos os ensaios ($N \in \{3, 5, 7, 10\}$), a parada de um nó subordinado gerou **0 novas eleições**.
   - O coordenador principal continuou operando normalmente e a estabilidade da liderança foi mantida em 100%.

---

## 5. Instruções para Reprodução

Para reproduzir todos os resultados descritos:

```bash
# 1. Executar os testes unitários da aplicação:
docker compose run --rm node1 pytest -q

# 2. Executar o experimento de referência (N=3, variação de heartbeat):
python3 scripts/run_experiment.py
cat experiments/results.csv
cat experiments/summary.csv

# 3. Executar a bateria de testes completa (N=3, 5, 7, 10 nós, 4 cenários, 5 repetições):
python3 scripts/run_test_battery.py --scenario all -n 3 5 7 10 --repetitions 5

# 4. Gerar os gráficos de convergência e de volume de mensagens:
python3 scripts/plot_battery.py
```

---

## 6. Discussão e Limitações

Os testes automatizados e a bateria de experimentos em contêineres Docker comprovaram que o sistema não trava após falhas (vivacidade) e que apenas o nó correto de maior ID assume a liderança sem conflitos (segurança).

Como limitações do estudo:
- Todos os contêineres compartilham a CPU e o subsistema de rede virtual do host, resultando em latências de comunicação sub-milissegúndicas.
- Timeouts fixos podem gerar falsos positivos sob sobrecarga extrema do host.
- O algoritmo Bully clássico não tolera partições de rede com nós isolados em partições disjuntas (*split-brain*). Para cenários com partições arbitrárias, algoritmos com quórum majoritário (como Raft ou Paxos) seriam recomendados.
