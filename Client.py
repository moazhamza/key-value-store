import sys

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport


def main():
    if len(sys.argv) != 2:
        print("Usage:", sys.argv[0], "SERVER_IP PORT_NUM")
    transport = TSocket.TSocket(sys.argv[1], sys.argv[2])
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    KVStore = KeyValueStore.Client(protocol)

    transport.open()

    line = input("Input command: ").lower()
    while line:
        key = input("Input the key: ")
        while not key.isdigit() or int(key) in range(0, 255):
            key = input("Please input a valid key: ")

        if line == "g" or line == "get":
            KVStore.get(int(key), ConsistencyLevel.ONE)

        elif line == 'p' or line == 'put':
            value = input("Input the string value: ")
            KVStore.put(int(key), value, ConsistencyLevel.ONE)

        line = input("Get or put?").lower()


if __name__ == "__main__":
    main()
