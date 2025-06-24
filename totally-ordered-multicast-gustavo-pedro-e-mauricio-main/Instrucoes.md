cada processo vai ter um relógio logico (temos que implementar o relógio logico nos pares e ele tem que atualizar ao receber dados)
acrescentar time stemp nas mensagens

acrescentar mensagens de ack da aplicação ( vai com time stemp também )

um par ao receber uma mensagem:

1º coisa: atualiza o relógio lógico
2° coisa: coloca a mensagem na fila
3° coisa: Se mensagem de dados -> enviar ack ( se for ack só faz 1º e 2º)
4º coisa: verifica se "destrava" a primeira mensagem da fila e entrega a mensagem para a aplicação

alguns pares na região 1, outras na região 2

GroupMngr 		-  só rodar
comparisonServer	-  só rodar
peerCommunicatorUDP.py	-  é o que temos que fazer a mudança no código

( não usar sleep )


objetivo: ao rodar aplicação, não ter mensagens desordenadas (a aplicação ja roda tudo, mas recebe mensagens desordenadas).
