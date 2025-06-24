from socket import *
from constMP import *
import threading
import random
import pickle
from requests import get
import heapq

handShakeCount = 0
PEERS = []
MESSAGE_QUEUE = []  # Min-heap based on (ts, sender, msg_number)
DELIVERED = set()
ACKS = dict()  # {(sender, msg_number): set(peer_ids)}
lamport_clock = 0
myself = None

sendSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket = socket(AF_INET, SOCK_DGRAM)
recvSocket.bind(('0.0.0.0', PEER_UDP_PORT))

serverSock = socket(AF_INET, SOCK_STREAM)
serverSock.bind(('0.0.0.0', PEER_TCP_PORT))
serverSock.listen(1)

def increment_clock():
    global lamport_clock
    lamport_clock += 1
    return lamport_clock

def update_clock(received_ts):
    global lamport_clock
    lamport_clock = max(lamport_clock, received_ts) + 1

def get_public_ip():
    ipAddr = get('https://api.ipify.org').content.decode('utf8')
    print('My public IP address is: {}'.format(ipAddr))
    return ipAddr

def registerWithGroupManager():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    ipAddr = get_public_ip()
    req = {"op": "register", "ipaddr": ipAddr, "port": PEER_UDP_PORT}
    msg = pickle.dumps(req)
    print('Registering with group manager: ', req)
    clientSock.send(msg)
    clientSock.close()

def getListOfPeers():
    clientSock = socket(AF_INET, SOCK_STREAM)
    print('Connecting to group manager: ', (GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    clientSock.connect((GROUPMNGR_ADDR, GROUPMNGR_TCP_PORT))
    req = {"op": "list"}
    msg = pickle.dumps(req)
    print('Getting list of peers from group manager: ', req)
    clientSock.send(msg)
    msg = clientSock.recv(2048)
    clientSock.close()
    peer_list = pickle.loads(msg)
    print('Got list of peers: ', peer_list)
    return peer_list

def try_deliver():
    global MESSAGE_QUEUE, ACKS, DELIVERED
    while MESSAGE_QUEUE:
        ts, sender, msg_number = MESSAGE_QUEUE[0]
        msg_id = (sender, msg_number)
        # Checa se todos os ACKs para essa mensagem já foram recebidos
        if all((s, m) in ACKS and len(ACKS[(s, m)]) == N for s, m, t in MESSAGE_QUEUE if t <= ts):
            heapq.heappop(MESSAGE_QUEUE)
            DELIVERED.add(msg_id)
            print(f'[DELIVERED] Message {msg_number} from {sender} with timestamp {ts}')
        else:
            break

class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('[LOG] Handler thread started. Waiting for handshakes...')
        global handShakeCount, lamport_clock, ACKS, myself

        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                print('[LOG] Received handshake from process', msg[1], '| Total handshakes:', handShakeCount, '/', N)

        print('[LOG] All handshakes received. Starting message processing...')
        stopCount = 0

        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)

            if msg[0] == -1:
                stopCount += 1
                print('[LOG] Received STOP signal from process', msg[1], '| Total STOPs:', stopCount, '/', N)
                if stopCount == N:
                    break

            elif isinstance(msg, tuple):
                if msg[0] == 'ACK':
                    _, ack_sender, ack_msg_number, ack_ts = msg
                    update_clock(ack_ts)
                    key = (ack_sender, ack_msg_number)
                    if key not in ACKS:
                        ACKS[key] = set()
                    ACKS[key].add(ack_sender)
                    print(f'[LOG] Received ACK from {ack_sender} for message {ack_msg_number} | Clock: {lamport_clock}')
                    try_deliver()

                elif len(msg) == 3:
                    sender, msg_number, ts = msg
                    update_clock(ts)
                    heapq.heappush(MESSAGE_QUEUE, (ts, sender, msg_number))
                    print(f'[LOG] Received message {msg_number} from {sender} with timestamp {ts} | Clock: {lamport_clock}')

                    # Envia ACK
                    ack_ts = increment_clock()
                    ack_msg = ('ACK', myself, msg_number, ack_ts)
                    ack_pack = pickle.dumps(ack_msg)
                    sendSocket.sendto(ack_pack, (PEERS[sender], PEER_UDP_PORT))
                    key = (myself, msg_number)
                    if key not in ACKS:
                        ACKS[key] = set()
                    ACKS[key].add(myself)  # próprio ACK
                    try_deliver()

        MESSAGE_QUEUE.sort()
        logFile = open('logfile' + str(myself) + '.log', 'w')
        logFile.writelines(str(list(DELIVERED)))
        logFile.close()
        print('[LOG] Messages saved to log file')

        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(list(DELIVERED))
        clientSock.send(msgPack)
        clientSock.close()
        print('[LOG] Ordered messages sent to server')

        handShakeCount = 0
        MESSAGE_QUEUE.clear()
        ACKS.clear()
        DELIVERED.clear()
        print('[LOG] Handler thread finished')
        exit(0)

def waitToStart():
    print('[LOG] Waiting for start signal from server...')
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    conn.send(pickle.dumps('Peer process ' + str(msg[0]) + ' started.'))
    conn.close()
    print('[LOG] Start signal received. My ID:', msg[0], '| Messages to send:', msg[1])
    return msg

print('[LOG] Starting peer process...')
registerWithGroupManager()

while True:
    print('\n[LOG] ===== NEW ITERATION =====')
    myself, nMsgs = waitToStart()

    if nMsgs == 0:
        print('[LOG] Termination signal received. Exiting...')
        exit(0)

    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()

    PEERS = getListOfPeers()

    for addrToSend in PEERS:
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    for msgNumber in range(nMsgs):
        lamport_ts = increment_clock()
        msg = (myself, msgNumber, lamport_ts)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
        print(f'[LOG] Sent message {msgNumber} with timestamp {lamport_ts}')

    for addrToSend in PEERS:
        msg = (-1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))

    msgHandler.join()
    print('[LOG] Handler finished. Ready for next iteration.')