import pickle
import threading
import time
from socket import *
from constMP import * 
import random
from requests import get # Para obter IP público

# Variáveis Globais para o algoritmo de Lamport
lamport_clock = 0
# message_buffer armazena tuplas: 
# ( (clock_val, sender_id_orig_DATA), 
#   (payload_sender_id, payload_msg_num), 
#   ack_sender_ids_set 
# )
message_buffer = [] 
# logList armazena tuplas entregues: (sender_id_orig_DATA, payload_msg_num)
logList = []

# Locks para proteger o acesso concorrente às variáveis globais
clock_lock = threading.Lock()
buffer_lock = threading.Lock()
log_list_lock = threading.Lock()

myself = -1  # ID deste peer, definido em waitToStart
PEERS_ADDRESSES = []  # Lista de IPs dos peers (obtida do GroupMngr)
ALL_PEER_IDS = [] # Lista de todos os IDs de peers (0 a N-1)
MY_PUBLIC_IP = None # IP público deste peer

# Sockets UDP
sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM) # Será vinculado em main


def get_my_public_ip():
    global MY_PUBLIC_IP
    if MY_PUBLIC_IP is None:
        try:
            MY_PUBLIC_IP = get('https://api.ipify.org').content.decode('utf8')
            print(f'Meu endereço IP público é: {MY_PUBLIC_IP}')
        except Exception as e:
            print(f"Não foi possível obter o IP público: {e}. Usando 127.0.0.1 para autocomparação.")
            MY_PUBLIC_IP = '127.0.0.1' # Fallback para testes locais
    return MY_PUBLIC_IP

def registerWithGroupManager():
    clientSock_gm = socket(AF_INET, SOCK_STREAM)
    print(f'Conectando ao gerenciador de grupo: {(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT)}')
    clientSock_gm.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    ipAddr = get_my_public_ip()
    # PEER_UDP_PORT para comunicação de dados
    req = {"op": "register", "ipaddr": ipAddr, "port": PEER_UDP_PORT} 
    msg = pickle.dumps(req)
    print(f'Registrando com o gerenciador de grupo: {req}')
    clientSock_gm.send(msg)
    clientSock_gm.close()

def unregisterWithGroupManager():
    clientSock_gm = socket(AF_INET, SOCK_STREAM)
    print(f'Conectando ao gerenciador de grupo: {(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT)}')
    clientSock_gm.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    ipAddr = get_my_public_ip()
    # PEER_UDP_PORT para comunicação de dados
    req = {"op": "unregister", "ipaddr": ipAddr} 
    msg = pickle.dumps(req)
    print(f'Desregistrando com o gerenciador de grupo: {req}')
    clientSock_gm.send(msg)
    clientSock_gm.close()

def getListOfPeers():
    global PEERS_ADDRESSES, ALL_PEER_IDS
    clientSock_gm = socket(AF_INET, SOCK_STREAM)
    print(f'Obtendo lista de peers do gerenciador de grupo: {(GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT)}')
    clientSock_gm.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    req = {"op": "list"}
    msg = pickle.dumps(req)
    clientSock_gm.send(msg)
    msg_recv = clientSock_gm.recv(4096) 
    PEERS_ADDRESSES = pickle.loads(msg_recv)
    print(f'Obtida lista de IPs dos peers: {PEERS_ADDRESSES}')
    clientSock_gm.close()
    
    if len(PEERS_ADDRESSES) != N:
        print(f"Erro: Esperava {N} peers, mas GroupManager retornou {len(PEERS_ADDRESSES)}")
        # Potencialmente sair ou tentar novamente
    
    # Assume que os IDs serão de 0 a N-1
    ALL_PEER_IDS = list(range(N)) 
    return PEERS_ADDRESSES

class DeliveryThread(threading.Thread):
    """
    Thread responsável por verificar o buffer de mensagens e entregar
    mensagens para a aplicação na ordem correta.
    """
    def __init__(self):
        threading.Thread.__init__(self)
        self.daemon = True 
        self.running = True

    def run(self):
        global message_buffer, logList, ALL_PEER_IDS
        print("DeliveryThread: Iniciada")
        while self.running:
            delivered_something_in_this_iteration = False
            with buffer_lock:
                if not message_buffer:
                    pass 
                else:
                    # Ordenar o buffer: primeiro por valor do relógio, depois por ID do remetente (para desempate)
                    message_buffer.sort(key=lambda item: (item[0][0], item[0][1]))
                    
                    if not message_buffer: # Verificar novamente após ordenação se esvaziou
                        continue

                    # print(f"Current buffer: {message_buffer}")

                    # Verificar se a mensagem no topo da fila pode ser entregue
                    msg_timestamp, msg_original_sender_id, msg_payload_tuple, received_acks = message_buffer[0]
                    
                    # Condição de entrega: ACK recebido de todos os N-1 outros peers
                    other_peer_ids_expected_for_ack = set(ALL_PEER_IDS) - {msg_original_sender_id}
                    # print(f"Received ACKs: {received_acks}")
                    
                    # Segunda condição de entrega: o payload da mensagem foi recebido
                    if other_peer_ids_expected_for_ack.issubset(received_acks) and msg_payload_tuple is not None:
                        delivered_msg_tuple_content = message_buffer.pop(0)
                        # O payload da mensagem é (original_sender_id, message_number)
                        # No log, armazenamos (sender_id_da_mensagem_DATA, message_number_original)
                        sender_of_data = delivered_msg_tuple_content[1] 
                        original_msg_number = delivered_msg_tuple_content[2][1]

                        with log_list_lock:
                            logList.append( (sender_of_data, original_msg_number) )
                        
                        print(f"DeliveryThread: Entregue MSG({delivered_msg_tuple_content[2]}) do Peer {sender_of_data} (ts: {msg_timestamp}). Tamanho do Log: {len(logList)}")
                        delivered_something_in_this_iteration = True
            
            if not delivered_something_in_this_iteration: 
                time.sleep(0.05)

    def stop(self):
        self.running = False
        print("DeliveryThread: Parando...")

class MsgHandler(threading.Thread):
    """
    Thread responsável por receber mensagens UDP (DATA e ACK),
    atualizar o relógio de Lamport e enviar ACKs.
    """
    def __init__(self, sock_to_use):
        threading.Thread.__init__(self)
        self.daemon = True
        self.sock = sock_to_use
        self.running = True

    def run(self):
        global lamport_clock, message_buffer, myself, PEERS_ADDRESSES
        print('MsgHandler: Iniciado. Aguardando mensagens...')
        
        while self.running:
            try:
                msgPack, addr = self.sock.recvfrom(4096) 
                recv_msg_unpickled = pickle.loads(msgPack)
                
                received_msg_clock_val = recv_msg_unpickled['timestamp'][0]
                with clock_lock:
                    lamport_clock = max(lamport_clock, received_msg_clock_val) + 1
                
                if recv_msg_unpickled['type'] == 'DATA':
                    data_ts = recv_msg_unpickled['timestamp']
                    data_sender = recv_msg_unpickled['sender_id']
                    data_payload = recv_msg_unpickled['payload'] 
                    
                    with buffer_lock:
                        duplicate_index = next((index for index, item in enumerate(message_buffer)
                            if item[0] == data_ts and item[1] == data_sender), -1)
                        is_duplicate = duplicate_index != -1

                        if not is_duplicate:
                            message_buffer.append( (data_ts, data_sender, data_payload, set()) )
                        elif message_buffer[duplicate_index][2] is None:
                            current_message = message_buffer[duplicate_index]
                            new_message = (current_message[0], current_message[1], data_payload, current_message[3])
                            message_buffer[duplicate_index] = new_message
                        
                        print(f"MsgHandler: MENSAGEM RECEBIDA de {data_sender} (payload: {data_payload}, ts: {data_ts})")
                    
                    with clock_lock:
                        lamport_clock += 1
                        ack_ts = (lamport_clock, myself)
                    
                    ack_msg_payload = {
                        'type': 'ACK',
                        'original_data_timestamp': data_ts, 
                        'original_data_sender_id': data_sender,
                        'timestamp': ack_ts, 
                        'ack_sender_id': myself 
                    }
                    ack_msg_packed = pickle.dumps(ack_msg_payload)
                    for peer_ip in PEERS_ADDRESSES:
                        # print(f"Enviando ACK para {peer_ip} referente à mensagem acima")
                        sendSocket.sendto(ack_msg_packed, (peer_ip, PEER_UDP_PORT))

                elif recv_msg_unpickled['type'] == 'ACK':
                    orig_data_ts = recv_msg_unpickled['original_data_timestamp']
                    orig_data_sender = recv_msg_unpickled['original_data_sender_id']
                    ack_sender = recv_msg_unpickled['ack_sender_id']
                    print(f"ACK recebido de {ack_sender} referente à mensagem de {orig_data_sender} com relógio {orig_data_ts[0]}")
                    
                    with buffer_lock:
                        added = False
                        for i, item_tuple in enumerate(message_buffer):
                            if item_tuple[0] == orig_data_ts and item_tuple[1] == orig_data_sender:
                                # Adiciona o ID do remetente do ACK ao conjunto de ACKs da mensagem DATA original
                                message_buffer[i][3].add(ack_sender) 
                                added = True
                                break

                        if not added:
                            ack_set = set()
                            ack_set.add(ack_sender)
                            message_buffer.append( (orig_data_ts, orig_data_sender, None, ack_set) )
                
                elif recv_msg_unpickled['type'] == 'STOP_HANDLER': 
                    print("MsgHandler: Recebido STOP_HANDLER. Terminando.")
                    self.running = False 
                    break

            except timeout: 
                if not self.running: 
                    break
                continue 
            except Exception as e:
                print(f"MsgHandler: Erro ao receber/processar mensagem: {e}")
                if not self.running: 
                    break
                continue
        print("MsgHandler: Parado.")

    def stop(self):
        self.running = False
        try:
            stop_msg_payload = {'type': 'STOP_HANDLER', 'timestamp': (0, myself) }
            stop_msg_packed = pickle.dumps(stop_msg_payload)
            sendSocket.sendto(stop_msg_packed, ('127.0.0.1', PEER_UDP_PORT))
        except Exception as e:
            print(f"MsgHandler: Erro ao enviar sinal de parada para si mesmo: {e}")

def send_application_messages(num_messages):
    """
    Envia um número especificado de mensagens DATA para o grupo.
    """
    global lamport_clock, myself, PEERS_ADDRESSES, message_buffer
    
    for msg_num in range(num_messages):
        time.sleep(random.randrange(10, 100) / 1000.0) 
        
        payload_content = (myself, msg_num) 
        
        current_ts_tuple = None
        with clock_lock:
            lamport_clock += 1
            current_ts_tuple = (lamport_clock, myself) 
        
        data_msg_dict = {
            'type': 'DATA',
            'sender_id': myself, 
            'timestamp': current_ts_tuple,
            'payload': payload_content 
        }
        data_msg_packed_for_send = pickle.dumps(data_msg_dict)

        with buffer_lock:
            message_buffer.append( (current_ts_tuple, myself, payload_content, set()) )
        
        print(f"Main: Peer {myself} enviando MSG {msg_num} (payload {payload_content}) com LC_TS {current_ts_tuple}")

        my_ip = get_my_public_ip()
        for peer_ip in PEERS_ADDRESSES:
            if peer_ip != my_ip: 
                 sendSocket.sendto(data_msg_packed_for_send, (peer_ip, PEER_UDP_PORT))

def waitToStart():
    """
    Espera o sinal de início do servidor de comparação via TCP.
    Define o ID global 'myself'.
    """
    global myself
    tcp_listener_sock = socket(AF_INET, SOCK_STREAM)
    tcp_listener_sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    tcp_listener_sock.bind(('0.0.0.0', PEER_TCP_PORT))
    tcp_listener_sock.listen(1)
    print(f'Peer TCP Listener: Aguardando sinal de início do servidor de comparação na porta {PEER_TCP_PORT}...')
    
    conn, addr = tcp_listener_sock.accept()
    print(f"Peer TCP Listener: Conexão de {addr}")
    msgPack = conn.recv(1024)
    msg_tuple = pickle.loads(msgPack) 
    
    temp_myself = msg_tuple[0]
    nMsgs = msg_tuple[1]
    
    with clock_lock: 
        myself = temp_myself 
    
    response_msg = f'Peer process {myself} ACKED start signal.'
    conn.send(pickle.dumps(response_msg))
    conn.close()
    tcp_listener_sock.close()
    print(f"Peer Main: Recebido sinal de início. Meu ID (myself) = {myself}, Num_Mensagens_Para_Enviar = {nMsgs}")
    return (myself, nMsgs)

# Início do código principal
if __name__ == "__main__":
    recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))
    recvSocket.settimeout(5) 

    registerWithGroupManager() 
    
    local_id_myself, num_messages_to_send_by_me = waitToStart() 
    
    getListOfPeers() 
    get_my_public_ip() 

    if not PEERS_ADDRESSES or len(PEERS_ADDRESSES) != N :
        print(f"ERRO CRÍTICO: Lista de peers não conforme o esperado. Esperava {N}, obteve {len(PEERS_ADDRESSES)}. Saindo.")
        exit(1)
    if myself == -1: # Deve ter sido definido por waitToStart
        print(f"ERRO CRÍTICO: myself (ID do peer) não definido por waitToStart. Saindo.")
        exit(1)

    print(f"Main: Peer {myself} inicializado. Enviará {num_messages_to_send_by_me} mensagens.")
    print(f"Main: Todos IPs dos peers: {PEERS_ADDRESSES}")
    print(f"Main: Todos IDs dos peers assumidos: {ALL_PEER_IDS}") # [0, 1, ..., N-1]

    msg_handler = MsgHandler(recvSocket)
    msg_handler.start()

    delivery_manager = DeliveryThread()
    delivery_manager.start()

    print(f"Main: Aguardando outros peers estarem prontos (atraso artificial de 5s)...")
    time.sleep(5) 

    if num_messages_to_send_by_me > 0:
        send_application_messages(num_messages_to_send_by_me)
    
    # Cada peer envia 'num_messages_to_send_by_me'.
    # Total de mensagens DATA no sistema = N * num_messages_to_send_by_me
    expected_total_delivered_messages = N * num_messages_to_send_by_me 
    
    print(f"Main: Aguardando todas as {expected_total_delivered_messages} mensagens serem entregues...")
    # Heurística para timeout: 2 segundos por mensagem esperada + 30s de folga
    wait_timeout_seconds = expected_total_delivered_messages * 5 + 30 
    start_wait_deliver = time.time()

    while True:
        with log_list_lock:
            current_delivered_count = len(logList)
        
        if current_delivered_count >= expected_total_delivered_messages:
            print(f"Main: Todas as {expected_total_delivered_messages} mensagens foram entregues.")
            break
        if time.time() - start_wait_deliver > wait_timeout_seconds:
            print(f"Main: TIMEOUT aguardando todas as mensagens. Entregues: {current_delivered_count}/{expected_total_delivered_messages}")
            # Mesmo com timeout, envia o que tem.
            break
        time.sleep(0.5)

    print("Main: Sinalizando para as threads pararem...")
    msg_handler.stop()
    delivery_manager.stop()
    
    msg_handler.join(timeout=5.0) # Espera a thread MsgHandler terminar
    delivery_manager.join(timeout=5.0) # Espera a thread DeliveryThread terminar
    print("Main: Threads auxiliares finalizadas.")

    unregisterWithGroupManager()

    print(f'Main: Enviando logList (tamanho: {len(logList)}) para o servidor de comparação...')
    clientSock_to_cs = socket(AF_INET, SOCK_STREAM)
    try:
        clientSock_to_cs.connect((SERVER_ADDR, SERVER_PORT))
        final_log_list_snapshot = []
        with log_list_lock:
             final_log_list_snapshot = list(logList)

        msgPack_log_to_send = pickle.dumps(final_log_list_snapshot)
        clientSock_to_cs.sendall(msgPack_log_to_send) 
    except Exception as e:
        print(f"Main: Erro ao enviar logList para o servidor de comparação: {e}")
    finally:
        clientSock_to_cs.close()
    
    print(f"Main: Peer {myself} finalizou a execução.")

    sendSocket.close()
    recvSocket.close()