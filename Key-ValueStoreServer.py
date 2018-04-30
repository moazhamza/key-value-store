import sys
sys.path.append('gen-py')

from keyValStore import KeyValueStore
from keyValStore.ttypes import SystemException

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from thrift.transport.TTransport import TTransportException

class StoreHandler:
    def __init__(self, nameIn, replicaNumIn, connectionsWaitingIn):
        self.store = {}
        self.name = nameIn
        self.number = replicaNumIn;
        self.connectionsWaiting = connectionsWaitingIn

        # Will be a list of tuples: (client, transport) after connection has been established
        self.establishedConnections = [None, None, None, None]

        # TODO: Implement delayed, threaded functions on an interval to establish a client connection to each replica server
        for i, rep in enumerate(self.connectionsWaiting):
            if i == self.number:
                continue

            # Callback function takes an iterable for arguments passed, so we put the connection index as an item in a list to pass in
            threading.Timer(5.0, self.__attemptConnection, args=(i)).start()

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
            self.put(key, value)

    def get(self, key, lvl=None):
        assert type(key) == int
        assert key >= 0
        assert key <= 255
        if key in self.store:
            return self.store[key]
        else:
            e = SystemException()
            e.message = "Key not in store"
            raise e

    def put(self, key, value, lvl=None):
        assert type(key) == int
        assert type(value) == str
        assert key >= 0
        assert key <= 255
        # TODO: Write in logger files all writes committed to the store
        self.logFile.write(str(key) + ':' + value + '\n')
        if key in self.store:
            self.store[key] = value
            return True
        else:
            self.store[key] = value
            return False

    def __attemptConnection(repNumber):
        assert repNumber != self.number

        # Attempting connection to item repNumber in self.connectionsWaiting

        try:
            # Make socket
            transport = TSocket.TSocket('localhost', self.connectionsWaiting[1])
            # Buffering is critical. Raw sockets are very slow
            transport = TTransport.TBufferedTransport(transport)
            # Wrap in a protocol
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            # Create a client to use the protocol encoder
            client = Calculator.Client(protocol)
            # Connect!
            transport.open()

            print('Connection established with ' + self.connectionsWaiting[0])
            self.establishedConnections[repNumber] = (client, transport)
        except TTransportException:
            threading.Timer(5.0, self.__attemptConnection, args=(i)).start()
            print('Rescheduling connection attempt with ' + self.connectionsWaiting[0])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Expected two arguments: name and port')
        exit(-1)

    # Handle command-line arguments
    replicaName = sys.argv[1]
    replicaPort = int(sys.argv[2])
    replicaNumber = -1
    replicaConnectionsToMake = [] # Will be a list of tuples: (name, port)

    # Read replicas.txt and prepare to connect to other replicas
    try:
        replicaFile = open('replicas.txt', 'r')

        for i, line in enumerate(replicaFile):
            repTup = tuple(line.split(' ')) # [0] = name, [1] = port
            if repTup[0] == replicaName: # Exclude if the line describes this replica
                replicaNumber = i
                continue

            replicaConnectionsToMake.append(repTup)
            print('Replica to connect to: ' + repTup[0] + ' on port ' + repTup[1])
    except IOError:
        print('Error: expected replicas.txt but did not find it. Not connected to any other replicas')

        if replicaNumber == -1:
            print('Error: Replica # is still -1 after looping through replicas.txt -- should not happen')

    handler = StoreHandler(replicaName, replicaNumber, replicaConnectionsToMake)
    processor = KeyValueStore.Processor(handler)
    transport = TSocket.TServerSocket(port=replicaPort)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')