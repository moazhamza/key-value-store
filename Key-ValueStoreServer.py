import glob
import sys
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
    def __init__(self):
        self.store = {}
        # TODO: Look for your logging file, populate the memory if it exists, create one if it does not

    def populate_from_mem(self, log):
        # TODO: Read through the logger, populating the memory as you go
        pass

    def get(self, key):
        if key in self.store:
            return self.store[key]
        else:
            return False

    def put(self, key, value):
        # TODO: Write in logger files all writes committed to the store
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

    replicaName = sys.argv[1]
    replicaPort = int(sys.argv[2])

    handler = StoreHandler()
    processor = Store.Processor(handler)
    transport = TSocket.TServerSocket(port=replicaPort)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('done.')