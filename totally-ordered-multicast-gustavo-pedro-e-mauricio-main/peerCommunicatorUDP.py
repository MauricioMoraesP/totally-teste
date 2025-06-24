from socket import *
from constMP import *  # -
import threading
import random
import time
import pickle
from requests import get


handShakeCount = 0
PEERS = []
LIST_MESSAGES = []
lamport_clock = 0  


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
    PEERS = pickle.loads(msg)
    print('Got list of peers: ', PEERS)
    clientSock.close()
    return PEERS


class MsgHandler(threading.Thread):
    def __init__(self, sock):
        threading.Thread.__init__(self)
        self.sock = sock

    def run(self):
        print('[LOG] Handler thread started. Waiting for handshakes...')
        global handShakeCount
        global LIST_MESSAGES
        global lamport_clock

        while handShakeCount < N:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)
            if msg[0] == 'READY':
                handShakeCount += 1
                print('[LOG] Received handshake from process', msg[1],
                      '| Total handshakes:', handShakeCount, '/', N)

        print('[LOG] All handshakes received. Starting message processing...')

        stopCount = 0
        while True:
            msgPack = self.sock.recv(1024)
            msg = pickle.loads(msgPack)

            
            if msg[0] == -1:
                stopCount += 1
                print('[LOG] Received STOP signal from process', msg[1],
                      '| Total STOPs:', stopCount, '/', N)
                if stopCount == N:
                    break
            elif isinstance(msg, tuple) and len(msg) == 3:
                sender, msg_number, ts = msg
                update_clock(ts)
                print(f'[LOG] Received message {msg_number} from process {sender} with timestamp {ts} | Updated local clock: {lamport_clock}')
                LIST_MESSAGES.append(msg)
                print('[LOG] Current message list size:', len(LIST_MESSAGES))

        
        print('[LOG] Starting message sorting...')
        LIST_MESSAGES.sort(key=lambda x: (x[2], x[0], x[1]))
        print('[LOG] Sorting completed. First few messages:')
        for msg in LIST_MESSAGES[:5]:
            print('   ', msg)

        logFile = open('logfile' + str(myself) + '.log', 'w')
        logFile.writelines(str(LIST_MESSAGES))
        logFile.close()
        print('[LOG] Messages saved to log file')

        print('[LOG] Preparing to send ordered messages to server...')
        clientSock = socket(AF_INET, SOCK_STREAM)
        clientSock.connect((SERVER_ADDR, SERVER_PORT))
        msgPack = pickle.dumps(LIST_MESSAGES)
        clientSock.send(msgPack)
        clientSock.close()
        print('[LOG] Ordered messages sent to server')

        handShakeCount = 0
        LIST_MESSAGES = []
        print('[LOG] Handler thread finished')
        exit(0)


def waitToStart():
    print('[LOG] Waiting for start signal from server...')
    (conn, addr) = serverSock.accept()
    msgPack = conn.recv(1024)
    msg = pickle.loads(msgPack)
    myself = msg[0]
    nMsgs = msg[1]
    conn.send(pickle.dumps('Peer process ' + str(myself) + ' started.'))
    conn.close()
    print('[LOG] Start signal received. My ID:', myself, '| Messages to send:', nMsgs)
    return (myself, nMsgs)



print('[LOG] Starting peer process...')
registerWithGroupManager()

while True:
    print('\n[LOG] ===== NEW ITERATION =====')
    (myself, nMsgs) = waitToStart()
    print('[LOG] I am process', myself)

    if nMsgs == 0:
        print('[LOG] Termination signal received. Exiting...')
        exit(0)

    print('[LOG] Creating message handler...')
    msgHandler = MsgHandler(recvSocket)
    msgHandler.start()

    PEERS = getListOfPeers()
    print('[LOG] Peer list obtained:', PEERS)

    print('[LOG] Sending handshakes to all peers...')
    for addrToSend in PEERS:
        print('   Sending to:', addrToSend)
        msg = ('READY', myself)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))


    print('[LOG] All handshakes confirmed. Proceeding to send messages...')

    
    print('[LOG] Starting to send', nMsgs, 'messages to each peer...')
    for msgNumber in range(nMsgs):
        lamport_ts = increment_clock()
        msg = (myself, msgNumber, lamport_ts)
        msgPack = pickle.dumps(msg)
        for addrToSend in PEERS:
            sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
        print(f'   Sent message {msgNumber} with timestamp {lamport_ts} to all peers')

    print('[LOG] Sending STOP signals to all peers...')
    for addrToSend in PEERS:
        msg = (-1, -1)
        msgPack = pickle.dumps(msg)
        sendSocket.sendto(msgPack, (addrToSend, PEER_UDP_PORT))
    print('[LOG] All STOP signals sent. Waiting for handler to finish...')

    msgHandler.join()
    print('[LOG] Handler finished. Ready for next iteration.')
