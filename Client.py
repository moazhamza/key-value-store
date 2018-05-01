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

    line = input("GET OR PUT: ").lower()
    while line:

        if line == "g" or line == "get":
            key = input("Input the key: ")
            while not key.isdigit() or int(key) not in range(0, 255):
                key = input("Please input a valid key: ")
            consistency = input("Consistency Level - ONE or QUORUM: ").lower()
            if consistency == '1' or consistency == 'one' or consistency == 'o':
                consistency = ConsistencyLevel.ONE
            elif consistency == '2' or consistency == 'quorum' or consistency == 'q':
                consistency = ConsistencyLevel.QUORUM
            try:
                print(KVStore.get(int(key), consistency))
            except SystemException as e:
                print(e.message)

        elif line == 'p' or line == 'put':
            key = input("Input the key: ")
            while not key.isdigit() or int(key) not in range(0, 255):
                key = input("Please input a valid key: ")
            value = input("Input the string value: ")
            consistency = input("Consistency Level - ONE or QUORUM: ").lower()
            while consistency != ConsistencyLevel.QUORUM and consistency != ConsistencyLevel.ONE:
                if consistency == '1' or consistency == 'one' or consistency == 'o':
                    consistency = ConsistencyLevel.ONE
                elif consistency == '2' or consistency == 'quorum' or consistency == 'q':
                    consistency = ConsistencyLevel.QUORUM
                else:
                    consistency = input("Invalid option - set valid consistency option: ")
            KVStore.put(int(key), value, consistency)

        else:
            print("Please input a valid command")
        line = input("GET or PUT: ").lower()


if __name__ == "__main__":
    main()
