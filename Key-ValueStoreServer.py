import sys
import threading
from enum import Enum
from time import sleep, time

sys.path.append('gen-py')

from keyValStore import KeyValueStore
from keyValStore.ttypes import SystemException, ConsistencyLevel, GetResult

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport.TTransport import TTransportException


class Status(Enum):
    NOT_STARTED = 0
    NOT_FOUND_FINISHED = 1
    FOUND_FINISHED = 2


class StoreHandler:
    def __init__(self, nameIn, replicaNumIn, connectionsWaitingIn):
        self.store = {}
        self.name = nameIn
        self.number = replicaNumIn
        self.connectionsWaiting = connectionsWaitingIn

        # Will be a list of tuples: (client, transport) after connection has been established
        self.establishedConnections = [None, None, None, None]
        for name, i, port in self.connectionsWaiting:
            if name == self.name:
                continue

            # Callback function takes an iterable for arguments passed,
            # so we put the connection index as an item in a list to pass in
            threading.Thread(target=self.__attemptConnection, name="Serving {}@{}".format(name, i, port),
                             args=[name, i, port]).start()

        # Look for log file, populate the memory if it exists, create one if it does not
        self.logFile = None
        fileName = nameIn + '.txt'
        try:
            self.logFile = open(fileName, 'r')
            self.__populate_from_mem(self.logFile)
            self.logFile.close()
            self.logFile = open(fileName, 'a')
        except IOError:
            print('Persistent storage file not found. Creating ' + fileName + ' now.')
            self.logFile = open(fileName, 'a+')

    def __populate_from_mem(self, log):
        for line in log:
            key, value = tuple(line.strip(':').split(':'))
            self.put(int(key), value)

    def get_aux(self, key):
        if key in self.store:
            result = GetResult()
            result.success = True
            result.result = self.store[key]
            return result
        else:
            result = GetResult()
            result.success = False
            result.value = ""
            return result

    def __get_from_different_replica(self, key, arr, finished, replica_num):
        if replica_num == self.number:
            if key in self.store:
                result = GetResult()
                result.success = True
                result.result = self.store[key]
                arr[replica_num] = result
                finished[replica_num] = Status.FOUND_FINISHED
            else:
                result = GetResult()
                result.success = False
                result.result = ""

                arr[replica_num] = result
                finished[replica_num] = Status.NOT_FOUND_FINISHED

        else:
            result = self.establishedConnections[replica_num][0].get_aux(key)
            arr[replica_num] = result
            if result.success:
                finished[replica_num] = Status.FOUND_FINISHED
            else:
                finished[replica_num] = Status.NOT_FOUND_FINISHED

    def get(self, key, lvl=None):
        if lvl == ConsistencyLevel.ONE:
            assert type(key) == int
            assert key >= 0
            assert key <= 255

            first_key_owner = key // 64

            finished = [Status.NOT_STARTED] * 4
            arr = [""] * 4
            for i in range(0, 3, 1):
                rep_num = (first_key_owner + i) % 4
                threading.Thread(
                    target=self.__get_from_different_replica,
                    name="Trying to get from {}".format(rep_num),
                    args=[key, arr, finished, rep_num]
                ).start()

            # Spin until someone gets the result, or they all finish without finding
            while (len(list(filter((lambda x: x is Status.FOUND_FINISHED), finished))) < 1) \
                    and len(list(filter(lambda x: x is Status.NOT_FOUND_FINISHED, finished))) < 3:
                pass

            found = len(list(filter((lambda x: x is Status.FOUND_FINISHED), finished))) > 0
            if found:
                for result in arr:
                    if result != "" and result.success:
                        return result.result
            else:
                e = SystemException()
                e.message = "Key not in store"
                return e

        elif lvl == ConsistencyLevel.QUORUM:
            pass

    def __put_to_different_replica(self, key, value, result_arr, finished_arr, rep_num, ts):
        if rep_num == self.number:
            self.store[key] = value

        else:
            result = self.establishedConnections[replica_num][0].get_aux(key)
            arr[replica_num] = result
            if result.success:
                finished[replica_num] = Status.FOUND_FINISHED
            else:
                finished[replica_num] = Status.NOT_FOUND_FINISHED

    def put(self, key, value, lvl=None):
        if lvl == ConsistencyLevel.ONE:
            assert type(key) == int
            assert type(value) == str
            assert key >= 0
            assert key <= 255

            self.logFile.write(str(key) + ':' + value + '\n')
            ts = int(time())
            first_key_owner = key // 64

            finished = [Status.NOT_STARTED] * 4
            arr = [""] * 4
            for i in range(0, 3, 1):
                rep_num = (first_key_owner + i) % 4
                threading.Thread(
                    target=self.__put_to_different_replica,
                    name="Trying to get from {}".format(rep_num),
                    args=[key, value, arr, finished, rep_num, ts]
                ).start()

            # Spin until someone gets the result, or they all finish without finding
            while (len(list(filter((lambda x: x is Status.FOUND_FINISHED), finished))) < 1) \
                    and len(list(filter(lambda x: x is Status.NOT_FOUND_FINISHED, finished))) < 3:
                pass

            found = len(list(filter((lambda x: x is Status.FOUND_FINISHED), finished))) > 0
            if found:
                for result in arr:
                    if result != "" and result.success:
                        return result.result
            else:
                e = SystemException()
                e.message = "Key not in store"
                return e
            # if first_key_owner == self.number:
            #     print("Put request for {} -> {}".format(key, value))
            #     if key in self.store:
            #         self.store[key] = value
            #         return True
            #     else:
            #         self.store[key] = value
            #         return False
            # else:
            #     return self.establishedConnections[first_key_owner][0].put(key, value, ConsistencyLevel.ONE)
        elif lvl == ConsistencyLevel.QUORUM:
            pass

    def __attemptConnection(self, name, num, port):
        # Attempting connection to item repNumber in self.connectionsWaiting
        assert name != self.name
        assert type(port) == int
        # Sleep to give a chance to start up other servers
        try:
            # Make socket
            transport = TSocket.TSocket('localhost', port)
            # Buffering is critical. Raw sockets are very slow
            transport = TTransport.TBufferedTransport(transport)
            # Wrap in a protocol
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            # Create a client to use the protocol encoder
            client = KeyValueStore.Client(protocol)
            # Connect!
            transport.open()
            self.establishedConnections[num] = (client, transport)
            print("connected to {}".format(name), flush=True)
        except TTransportException:
            print('Rescheduling connection attempt with ' + name + "@" + str(port), flush=True)
            sleep(5)
            self.__attemptConnection(name, num, port)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Expected two arguments: name and port')
        exit(-1)

    # Handle command-line arguments
    replicaName = sys.argv[1]
    replicaPort = int(sys.argv[2])
    replicaNumber = -1
    replicaConnectionsToMake = []  # Will be a list of tuples: (name, port)

    # Read replicas.txt and prepare to connect to other replicas
    try:
        replicaFile = open('replicas.txt', 'r')

        for i, line in enumerate(replicaFile):
            name, port = tuple(line.split(' '))  # [0] = name, [1] = port
            if name == replicaName:  # Exclude if the line describes this replica
                replicaNumber = i
                continue
            replicaConnectionsToMake.append((name, i, int(port)))
    except IOError:
        print('Error: expected replicas.txt but did not find it. Not connected to any other replicas')
        assert replicaNumber != -1

    handler = StoreHandler(replicaName, replicaNumber, replicaConnectionsToMake)
    processor = KeyValueStore.Processor(handler)
    transport = TSocket.TServerSocket(port=replicaPort)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')
