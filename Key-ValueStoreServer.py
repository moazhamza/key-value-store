import glob
import sys
import os
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

#from tutorial import Calculator
#from tutorial.ttypes import InvalidOperation, Operation

from shared.ttypes import SharedStruct

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class StoreHandler:
    def __init__(self, nameIn, connectionsWaitingIn):
        self.store = {}
        self.name = nameIn
        self.connectionsWaitingIn = connectionsWaitingIn

        # Look for log file, populate the memory if it exists, create one if it does not
        self.logFile = None
        fileName = nameIn + '.txt'
        try:
            self.logFile = open(fileName, 'ra')
            self.__populate_from_mem(self.logFile)
        except IOError:
            print 'Persitent storage file not found. Creating ' + fileName + ' now.'
            self.logFile = open(fileName, 'rw+')

    def __populate_from_mem(self, log):
        # TODO: Read through the logger, populating the memory as you go
        for line in log:
            # TODO: Populate the store using the lines of the file (Need to settle on format to write to the file
            key, value = tuple(line.strip(':').split(':'))
            self.put(key, value)

    def get(self, key, lvl=None):
        assert type(key) == int
        assert key >= 0
        assert key <= 255
        if key in self.store:
            return self.store[key]
        else:
            return False

    def put(self, key, value, lvl=None):
        assert type(key) == int
        assert type(value) == str
        assert key >= 0
        assert key <= 255
        # TODO: Write in logger files all writes committed to the store
        self.logFile.write(str(key) + ':' + value)
        if key in self.store:
            self.store[key] = value
            return True
        else:
            self.store[key] = value
            return False

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'Expected two arguments: name and port'
        return

    # Handle command-line arguments
    replicaName = sys.argv[1]
    replicaPort = int(sys.argv[2])
    replicaConnectionsToMake = [] # Will be a list of tuples: (name, port)

    # Read replicas.txt and prepare to connect to other replicas
    try:
        replicaFile = open('replicas.txt', 'r')

        for line in replicaFile:
            repTup = tuple(line.split(' ')) # [0] = name, [1] = port
            if repTup[0] == replicaName: # Exclude if the line describes this replica
                continue

            replicaConnectionsToMake.append(repTup)
            print 'Replica to connect to: ' + repTup[0] + ' on port ' + repTup[1]
    except IOError:
        print 'Error: expected replicas.txt but did not find it. Not connected to any other replicas'



    handler = StoreHandler(replicaName, replicaConnectionsToMake)
    processor = Store.Processor(handler)
    transport = TSocket.TServerSocket(port=replicaPort)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')