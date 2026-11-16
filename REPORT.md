# Implementação e Avaliação do Algoritmo de Eleição Bully com Detector de Falhas Baseado em Heartbeat

**Autores:**  
- Rafael Carro Gaudim — RA: 240879  
- Douglas Henrique Pereira — RA: 245202  
**Disciplina:** MC714 (Sistemas Distribuídos) — 2º Semestre de 2026 — Instituto de Computação, UNICAMP  
**Repositório:** [https://github.com/RafaelCarro/MC714_Lab02](https://github.com/RafaelCarro/MC714_Lab02)  
**Vídeo de Apresentação:** [https://youtu.be/bssPcbm8a4E](https://youtu.be/bssPcbm8a4E)

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

### 3.2. Máquina de Estados e Variáveis Locais (`NodeState`)
Cada nó lógico opera como uma máquina de estados finitos que transita entre três papéis principais:
- **Seguidor (*Follower*):** Papel padrão de qualquer nó subordinado ($ID < ID_{coord}$). Monitora o líder periodicamente via `Heartbeat` e rastreia o contador de falhas consecutivas.
- **Candidato (*Candidate*):** Estado transitório ativado quando o nó atinge o limiar de falhas de heartbeat ou recebe uma convocação `StartElection`. O nó define `election_active = True`, incrementa seu contador monotônico `current_election_id`, convoca os nós de maior ID e aguarda respostas dentro do prazo.
- **Coordenador (*Leader*):** Estado assumido quando nenhum nó superior responde. O nó suspende o envio ativo de heartbeats, difunde `AnnounceCoordinator` aos demais nós e passa a responder passivamente às verificações recebidas com `Pong(ok=True)`.

O estado em memória (`NodeState`) agrega as variáveis `coordinator_id`, `election_active`, `current_election_id` e o contador de timeouts. Todas as transições críticas e leituras concorrentes (entre threads gRPC e a thread de monitoramento) são protegidas por exclusão mútua via `threading.Lock`.

### 3.3. Detector de Falhas Heartbeat
- Nós subordinados executam uma thread dedicada enviando `Heartbeat` a cada $T$ segundos (`HEARTBEAT_INTERVAL_SECONDS`).
- Uma resposta válida zera o contador de falhas.
- Um timeout ou erro de conexão incrementa o contador. Ao atingir $F = 2$ (`HEARTBEAT_FAILURE_THRESHOLD`), o nó assume que o coordenador falhou, invalida seu líder local e inicia o algoritmo Bully.

### 3.4. Algoritmo Bully Otimizado
1. **Disparo:** O iniciador incrementa monotonicamente seu contador de eleição local e transmite `StartElection(node_id, election_id)` a todos os nós com $ID > ID_{local}$.
2. **Delegação:** Qualquer nó de ID superior que receba a mensagem responde afirmativamente (`accepted = true`) e inicia concorrentemente sua própria rodada eleitoral.
3. **Vitória:** Se o iniciador não receber nenhuma resposta `accepted = true` dentro do prazo de espera de RPC, declara-se coordenador e envia `AnnounceCoordinator` a todos os nós do cluster.
4. **Descarte de Mensagens Obsoletas:** Mensagens com `election_id` inferior ao registrado localmente são descartadas para impedir condições de corrida por mensagens defasadas.
5. **Otimização de Inicialização (*Cold Start*):** Para evitar uma tempestade quadrática de mensagens durante a subida conjunta do cluster, apenas nós cujo $ID \ge ID_{coord}$ disparam eleição no boot. Se o coordenador configurado já estiver saudável, os seguidores iniciam diretamente o envio de heartbeats, reduzindo a complexidade de boot para $\mathcal{O}(N)$.

### 3.5. Rastreamento Transparente de Métricas de Mensagens
Para avaliar a complexidade empírica $\mathcal{O}(N^2)$ sem adulterar o contrato de interface `node.proto` (preservando total compatibilidade com corretores automáticos), o servidor contabiliza chamadas emitidas e recebidas via logs estruturados `[MSG] <TIPO_MENSAGEM>`. Esse log é habilitado seletivamente via variável de ambiente `ENABLE_METRICS_LOGS=1`.

---

## 4. Avaliação Experimental

A experimentação foi estruturada em duas frentes automatizadas e reprodutíveis. A injeção de falhas nos experimentos cronometra a latência a partir do encerramento efetivo do container (`docker compose stop -t 1`), isolando a latência do daemon do Docker do tempo de resposta do algoritmo distribuído.

### 4.1. Experimento de Referência: Variação de Heartbeat ($N=3$)
Executado via `scripts/run_experiment.py`, avaliando três intervalos de heartbeat ($T \in \{1, 2, 3\}$ s) com deadline de 2,0 s e 3 repetições:

| Intervalo Heartbeat (s) | Repetições | Sucesso Normal | Sucesso Falha | Média Convergência (s) | Desvio Padrão (s) | Mediana p50 (s) | Percentil p95 (s) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1.0** | 3 | 100% | 100% | **1,186** | 0,436 | 0,973 | 1,616 |
| **2.0** | 3 | 100% | 100% | **2,932** | 0,118 | 2,869 | 3,048 |
| **3.0** | 3 | 100% | 100% | **4,900** | 0,064 | 4,935 | 4,938 |

*Observação:* Com a latência de término do contêiner isolada, o tempo de convergência para $T=1.0\,\text{s}$ ($1,186\,\text{s}$) é plenamente compatível com o cenário de escalabilidade ($1,177\,\text{s}$), refletindo puramente o tempo de detecção da ausência de heartbeats e o anúncio da nova liderança.

---

### 4.2. Bateria Modular de Testes ($N \in \{3, 5, 7, 10\}$)
Executada via `scripts/run_test_battery.py` com 10 repetições para o cenário de escalabilidade, avaliando latências e a quantidade de mensagens trocadas na eleição.

#### A) Escalabilidade e Tempo de Convergência ($T=1{,}0\,\text{s}, F=2$)

| Cenário | $N$ Nós | Repetições | Latência Média (s) | Desvio Padrão (s) | p50 (s) | p95 (s) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Escalabilidade** | 3 | 10 | 1,177 | 0,123 | 1,094 | 1,325 |
| **Escalabilidade** | 5 | 10 | 1,013 | 0,349 | 1,107 | 1,347 |
| **Escalabilidade** | 7 | 10 | 1,221 | 0,155 | 1,232 | 1,449 |
| **Escalabilidade** | 10 | 10 | 1,149 | 0,127 | 1,094 | 1,336 |

**Análise:** Ao medir a convergência puramente a partir do momento em que o processo do coordenador é interrompido, a latência média permaneceu notavelmente plana entre **$1{,}013\,\text{s}$ e $1{,}221\,\text{s}$** para todos os tamanhos de cluster. Isso comprova que a detecção depende do período dos heartbeats independentes, enquanto o overhead de rede local gRPC é desprezível.

#### B) Complexidade de Mensagens ($\mathcal{O}(N^2)$)

| $N$ Nós | Repetições | Média Total Mensagens | Election | OK | Coordinator | ACK |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **3** | 10 | **5,7** | 1,5 | 0,4 | 1,8 | 2,0 |
| **5** | 10 | **12,0** | 2,3 | 1,3 | 3,6 | 4,8 |
| **7** | 10 | **25,1** | 7,0 | 4,6 | 5,6 | 7,9 |
| **10** | 10 | **78,8** | 13,1 | 18,9 | 39,9 | 6,9 |

No cenário completo com falha e recuperação (*failback*) do coordenador original (**Baseline**):
- $N=3 \implies \mathbf{8{,}0}$ mensagens;
- $N=5 \implies \mathbf{25{,}8}$ mensagens;
- $N=7 \implies \mathbf{33{,}0}$ mensagens; e
- $N=10 \implies \mathbf{69{,}8}$ mensagens.

*Nota sobre a variabilidade em $N=10$:* Observou-se um elevado desvio-padrão no total de mensagens para $N=10$ (com medições variando de 22 a 195 mensagens). Essa dispersão decorre da assimetria do algoritmo Bully: se o segundo maior nó ($N-1$) detecta a falha primeiro, ele contata apenas o líder e se proclama com sobrecarga mínima ($\mathcal{O}(N)$); se nós inferiores detectam primeiro, eles disparam a convocação em cascata a todos os superiores, provocando a tempestade quadrática $\mathcal{O}(N^2)$.

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

### 5.1. Ambiente de Execução e Configuração do Host
Todos os experimentos foram conduzidos em um único equipamento host com as seguintes especificações:
- **Processador:** Intel(R) Core(TM) i5-7200U CPU @ 2.50GHz (2 núcleos físicos, 4 threads);
- **Memória RAM:** 8 GB DDR4;
- **Sistema Operacional:** Ubuntu 26.04 LTS (Kernel Linux 7.0 x86_64);
- **Ferramentas:** Docker Engine 29.9.3, Docker Compose v5.7.1, Python 3.14.4, gRPC/Protobuf 1.76.0 e Pytest 8.4.2;
- **Aquecimento (*Warm-up*):** Estabilização de 5 segundos após a inicialização dos contêineres para troca de heartbeats válidos antes da injeção de falhas;
- **Duração dos ensaios:** 15 a 20 segundos por repetição.

### 5.2. Comandos de Reprodução
Para reproduzir todos os resultados descritos:

```bash
# 1. Executar os testes unitários da aplicação:
docker compose run --rm node1 pytest -q

# 2. Executar o experimento de referência (N=3, variação de heartbeat):
python3 scripts/run_experiment.py
cat experiments/results.csv
cat experiments/summary.csv

# 3. Executar a bateria de testes completa (N=3, 5, 7, 10 nós):
python3 scripts/run_test_battery.py --scenario all -n 3 5 7 10 --repetitions 10

# 4. Gerar os gráficos de convergência e de volume de mensagens:
python3 scripts/plot_battery.py
```

---

## 6. Discussão e Limitações

Os testes automatizados e a bateria de experimentos em contêineres Docker comprovaram que o sistema não trava após falhas (vivacidade) e que apenas o nó correto de maior ID assume a liderança sem conflitos (segurança).

Como limitações fundamentais do estudo:
- **Domínio Único de Falha:** Embora cada nó execute isolado em seu próprio contêiner, todos compartilham a mesma máquina host física e o mesmo kernel do SO. Uma falha de hardware ou reinicialização do daemon encerra simultaneamente todos os nós, não constituindo domínios de falha independentes.
- **Canal Virtual e Latência:** A comunicação via ponte de rede (*bridge network*) do Docker ocorre em memória do host, resultando em latências sub-milissegúndicas e perda de pacotes nula, diferindo de enlaces reais de longa distância (WAN) sujeitos a *jitter*, perdas assimétricas e partições (*split-brain*).
- **Contenção de CPU:** A concorrência por ciclos de processamento e agendamento no host sob rajadas intensas de mensagens pode atrasar temporariamente o tratamento de callbacks RPC, ressaltando a necessidade de calibrar timeouts e tolerância a atrasos transitórios.

---

## 7. Referências

1. M. van Steen and A. S. Tanenbaum, *Distributed Systems*, 4th ed., distributed-systems.net, 2023.
