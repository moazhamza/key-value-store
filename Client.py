import sys

sys.path.append('gen-py')

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from keyValStore import KeyValueStore
from keyValStore.ttypes import ConsistencyLevel
from keyValStore.ttypes import SystemException


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
        while not key.isdigit() or int(key) not in range(0, 255):
            key = input("Please input a valid key: ")

        if line == "g" or line == "get":
            try:
                print(KVStore.get(int(key), ConsistencyLevel.ONE))
            except SystemException as e:
                print(e.message)

        elif line == 'p' or line == 'put':
            value = input("Input the string value: ")
            KVStore.put(int(key), value, ConsistencyLevel.ONE)

        else:
            print("Please input a valid command")
        line = input("Input command: ").lower()


if __name__ == "__main__":
    main()
