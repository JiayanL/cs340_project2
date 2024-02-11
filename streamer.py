# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.format = 'i'
        self.recvbuffer = {}
        self.ack = 0
        self.sequence_number = 0

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Support sending data larger than 1472 bytes by breaiking data_bytes into chunks 
        bytes_list = []
        sequence_number = 0
        for i in range(0, len(data_bytes), 1468):
            # Old approach
            # bytes_list.append(data_bytes[i:i + 1472])
            # Chunk data and add sequence number in header
            data_byte = data_bytes[i:i+1468]
            header = struct.pack(self.format, self.sequence_number) 
            packet = header + data_byte
            bytes_list.append(packet)
            print(f"sequence number: {self.sequence_number}, data: {data_byte}")
            self.sequence_number += 1

        # for now I'm just sending the raw application-level data in one UDP payload
        for data_byte in bytes_list:
            self.socket.sendto(data_byte, (self.dst_ip, self.dst_port))

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # parse the sequence number and wait to return packets in order
        # check each segment by comparing it to next sequence number expected (ACK)
        # if sequence number received != that number, store in buffer and continue waiting
        # if sequence number you expected is received or in your buffer, return that data
        # busy wait with while True
        # Inside the loop, every time you receive a packet from recvfrom, store it in the receive buffer
        # if a packet is received that mathces ACK number, return data from recv, exiting the loop
        while True: 
            if self.ack in self.recvbuffer:
                print(f"ack found!: {self.ack}")
                packet = self.recvbuffer[self.ack]
                del self.recvbuffer[self.ack]
                self.ack += 1
                return packet
            # this sample code just calls the recvfrom method on the LossySocket
            data, addr = self.socket.recvfrom()
            header_length = struct.calcsize(self.format)
            sequence_number = struct.unpack(self.format, data[:header_length])[0]
            data_byte = data[header_length:]
            self.recvbuffer[int(sequence_number)] = data_byte

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                # store data in the receive buffer
            except Exception as e:
                print("Listener died!")
                print(e)

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
