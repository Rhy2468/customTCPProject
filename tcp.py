import socket
import struct
import time
import random
import heapq
import threading
from collections import defaultdict

WINDOW_SIZE = 5 
SEQ_OVERFLOW_NUM = 100 
PACKET_FORMAT = "IIHBBI" 

def checkSum(seqNum, ackNum, flags, priority, windowSize, payload):
    #Sum up all the fields to make up checksum 
    checksum = seqNum + ackNum + flags + priority + windowSize + sum(payload)  

    #Make sure checksum fits within 32 bits and return the value 
    if checksum >= 2**32:
        checksum = checksum % (2**32)
    return checksum

def makePacket(seqNum, ackNum, flags, priority, windowSize, payload):
    #Convert the message into bytes and then create the Checksum 
    payloadinBytes = payload.encode('utf-8')  
    checksum = checkSum(seqNum, ackNum, flags, priority, windowSize, payloadinBytes)  

    #Return the payload in bytes preceded with packet information 
    return struct.pack(PACKET_FORMAT, seqNum, ackNum, flags, priority, windowSize, checksum) + payloadinBytes

def getPacket(packet):
    #Calculate header size and GEt header from packet 
    header_size = struct.calcsize(PACKET_FORMAT)
    header = packet[:header_size] 

    #Separate fields into own variables 
    seqNum, ackNum, flags, priority, windowSize, checksum = struct.unpack(PACKET_FORMAT, header)  

    #Extract and decode payload, return packet fields 
    payload_start = header_size
    payload = packet[payload_start:].decode('utf-8')  
    return seqNum, ackNum, flags, priority, windowSize, checksum, payload

class SenderWindow:
    def __init__(self, size):
        self.size = size
        self.base = 0
        self.nextSeqNum = 0
        self.packets = {}
        self.ackCount = defaultdict(int)
        
    def is_full(self):
        return self.nextSeqNum - self.base >= self.size
    
    def can_send(self):
        return not self.is_full() and self.nextSeqNum < SEQ_OVERFLOW_NUM

#Sender Function 
def sender():
    #Create UDP socket and create timeout for receiving ACKs 
    senderSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    senderSocket.settimeout(2)

    #Variables
    window = SenderWindow(WINDOW_SIZE)
    priorityQueue = []
    lostPackets = set()
    serverAddress = ('localhost', 24680)

    #Continue sending until all packets are sent 
    while window.base < SEQ_OVERFLOW_NUM:
        # Fill window with new packets
        while window.can_send():
            seqNum = window.nextSeqNum
            payload = f"Message {seqNum}"
            priority = int(seqNum % 3 == 0)
            packet = makePacket(seqNum, 0, 0, priority, WINDOW_SIZE, payload)
            window.packets[seqNum] = packet
            heapq.heappush(priorityQueue, (1 - priority, seqNum, packet))
            window.nextSeqNum += 1

        #Send packets through connection 
        while priorityQueue and priorityQueue[0][1] < window.nextSeqNum:
            #Obtain packet with highest priority 
            priority, seqNum, packet = heapq.heappop(priorityQueue)  

            #Note: we are simulating the chances of losing a packet in this case and for testing 
            if not (random.random() < 0.1):  
                #Send the packet to the receiver and print details 
                senderSocket.sendto(packet, serverAddress)  
                print(f"Sender: sent {getPacket(packet)}\n")  
            else:
                print("Sender: packet loss\n")
                lostPackets.add(seqNum) 
                heapq.heappush(priorityQueue, (1 - priority, seqNum, packet))
        
        #Deal with acknowledgements 
        try:
            #Receive ACK packet and extract fields (Will only use some)
            
            data, addr = senderSocket.recvfrom(1024) 
            ackSeqNum, ackNum, flags, priority, windowSize, checksum, payload = getPacket(data)  
            #Print ACK
            print(f"Sender: received ACK for {ackSeqNum}\n") 

            #Ensure packets inside window have been acknowledged 
            if ackSeqNum >= window.base:
                #decrement packets currently being sent and move window forward 
                for i in range(window.base, ackSeqNum + 1):
                    window.packets.pop(i, None)
                    lostPackets.discard(i)
                window.base = ackSeqNum + 1

        #If socket times out 
        except socket.timeout:
            print("Sender: socket timed-out, Re-attempting\n")
            # Retransmit unacked packets
            for seqNum in sorted(window.packets.keys()):
                if seqNum >= window.base:
                    packet = window.packets[seqNum]
                    priority = int(seqNum % 3 == 0)
                    heapq.heappush(priorityQueue, (1 - priority, seqNum, packet))
    #Close socket 
    senderSocket.close()

#Receiver Function 
def receiver():
    #Create UDP connection and connect
    serverAddress = ('localhost', 24680)  
    recvSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  
    recvSocket.bind(serverAddress)  

    #Variables 
    expectedSeq = 0
    receivedBuffer = {} 
    while True:
        #Attempt to receive packet data 
        try:
            #Attemp packet receive and get data, client address, and fields 
            data, clientAddress = recvSocket.recvfrom(1024)
            seqNum, ackNum, flags, priority, windowSize, checksum, payload = getPacket(data) 
            
            #Get checksum and validate checksum 
            checksumCalced = checkSum(seqNum, 0, flags, priority, windowSize, payload.encode('utf-8'))
            if checksumCalced != checksum:
                print("Receiver: checksum mismatch! Discarding packet.\n")
                continue

            # Send ACK for the received packet
            ackPacket = makePacket(seqNum, 0, 0, 0, WINDOW_SIZE, "")
            recvSocket.sendto(ackPacket, clientAddress)

            #If packed is expected packet then process it 
            if seqNum == expectedSeq:
                print(f"Receiver: received {payload} (Priority: {priority})\n")
                expectedSeq += 1
                
                # Process any buffered packets
                while expectedSeq in receivedBuffer:
                    print(f"Receiver: received (from buffer): {receivedBuffer[expectedSeq]}\n")
                    del receivedBuffer[expectedSeq]
                    expectedSeq += 1
            elif seqNum > expectedSeq:
                print(f"Receiver: out-of-order packet received. Expected: {expectedSeq}, Received: {seqNum}\n")
                receivedBuffer[seqNum] = payload
            else:
                print(f"Receiver: duplicate packet received: {seqNum}\n")
                
        except socket.timeout:
            continue
    
    #close connection 
    recvSocket.close()  

if __name__ == "__main__":
    # Start the receiver in a separate thread, give time for startup and then start sender  
    threading.Thread(target=receiver, daemon=True).start()
    time.sleep(1)
    sender()
