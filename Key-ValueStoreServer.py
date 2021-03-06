#!/usr/bin/env python

import sys
import threading
import glob
from enum import Enum
from time import sleep, time

sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

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
    def __init__(self, nameIn, replicaNumIn, connectionsWaitingIn, consistencyMechanismIn):
        self.store = {}
        self.time = {}
        self.name = nameIn
        self.number = replicaNumIn
        self.connectionsWaiting = connectionsWaitingIn
        self.consistencyMechanism = consistencyMechanismIn
        # 0 = Read repair, 1 = Hinted handoff
        self.hintedHandoff = [[], [], [], []] # Essentially queues for handoff items
        # Each of the four lists will contain elements of form: (key, value, ts)

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
            self.store[key] = value.strip()
            self.time[key] = 0

    # RPC from coordinator to replica for their data
    def get_aux(self, issuing_replica, key):
    	if self.consistencyMechanism == 1 and len(self.hintedHandoff[issuing_replica]) > 0:
            self.__dumpHintedHandoff(issuing_replica)
            
        if key in self.store:
            result = GetResult()
            result.success = True
            result.result = self.store[key]
            result.time = self.time[key]
            return result
        else:
            result = GetResult()
            result.success = False
            result.value = ""
            return result

    # Delegate async function to fetch other replica's data (or yours)
    def __get_from_different_replica(self, key, arr, finished, replica_num):
        if replica_num == self.number:
            if key in self.store:
                result = GetResult()
                result.success = True
                result.result = self.store[key]
                result.time = self.time[key]
                arr[replica_num] = result
                finished[replica_num] = Status.FOUND_FINISHED
            else:
                result = GetResult()
                result.success = False
                result.result = ""

                arr[replica_num] = result
                finished[replica_num] = Status.NOT_FOUND_FINISHED

        else:
            if self.establishedConnections[replica_num] != None:
	        result = self.establishedConnections[replica_num][0].get_aux(self.number, key)
	        arr[replica_num] = result
	        if result.success:
	            finished[replica_num] = Status.FOUND_FINISHED
	        else:
	            finished[replica_num] = Status.NOT_FOUND_FINISHED

    @staticmethod
    def __get_most_recent(arr):
        most_recent = ""
        for result in arr:
            if result != "":
                if most_recent == "":
                    most_recent = result
                else:
                    if result.time > most_recent.time:
                        most_recent = result
        return most_recent

    # Coordinator get
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
                    	if self.consistencyMechanism == 0:
                    	    self.__performReadRepair(key, result.result, result.time)
                    	
                        return result.result
            else:
                e = SystemException()
                e.message = "Key not in store"
                raise e

        elif lvl == ConsistencyLevel.QUORUM:
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
            while (len(list(filter((lambda x: x is Status.FOUND_FINISHED), finished))) < 2) \
                    and len(list(filter(lambda x: x is Status.NOT_FOUND_FINISHED, finished))) < 3:
                pass

            found = len(list(filter((lambda x: x is Status.FOUND_FINISHED), finished))) >= 2
            if found:
            	resultStruct = self.__get_most_recent(arr)
            	if self.consistencyMechanism == 0:
            	    self.__performReadRepair(key, resultStruct.result, resultStruct.time)
                return resultStruct.result
            else:
                e = SystemException()
                e.message = "Key not in store"
                raise e

    def put_aux(self, issuing_replica, key, value, ts):
        self.logFile.write(str(key) + ':' + value + '\n')
        self.store[key] = value
        self.time[key] = ts
        
        if self.consistencyMechanism == 1 and len(self.hintedHandoff[issuing_replica]) > 0:
            self.__dumpHintedHandoff(issuing_replica)
            
        return True

    def __put_to_different_replica(self, key, value, finished_arr, rep_num, ts):
        if rep_num == self.number:
            self.store[key] = value
            self.time[key] = ts
            finished_arr[rep_num] = True
        else:
            result = ""
            try:
                result = self.establishedConnections[rep_num][0].put_aux(self.number, key, value, ts)
                
                if result:
                    finished_arr[rep_num] = True
                else:
                    raise SystemException()
            except:
            	if self.consistencyMechanism == 1: # Hinted handoff
            	    self.hintedHandoff[rep_num].append((key,value,ts))
            	    
            	    finished_arr[rep_num] = True
            	else:
                    finished_arr[rep_num] = False

    def put(self, key, value, lvl=None):
        if lvl == ConsistencyLevel.ONE:
            assert type(key) == int
            #assert type(value) == str
            assert key >= 0
            assert key <= 255

            self.logFile.write(str(key) + ':' + value + '\n')
            ts = int(time())
            first_key_owner = key // 64

            finished = [False] * 4
            for i in range(0, 3, 1):
                rep_num = (first_key_owner + i) % 4
                threading.Thread(
                    target=self.__put_to_different_replica,
                    name="Trying to put to {}".format(rep_num),
                    args=[key, value, finished, rep_num, ts]
                ).start()

            # Spin until someone gets the result, or they all finish without finding
            while len(list(filter((lambda x: x is True), finished))) < 1:
                pass

            found = len(list(filter((lambda x: x is True), finished))) > 0
            if found:
                return True
            else:
                return False
        elif lvl == ConsistencyLevel.QUORUM:
            assert type(key) == int
            #assert type(value) == str
            assert key >= 0
            assert key <= 255

            self.logFile.write(str(key) + ':' + value + '\n')
            ts = int(time())
            first_key_owner = key // 64

            finished = [False] * 4
            for i in range(0, 3, 1):
                rep_num = (first_key_owner + i) % 4
                threading.Thread(
                    target=self.__put_to_different_replica,
                    name="Trying to put to {}".format(rep_num),
                    args=[key, value, finished, rep_num, ts]
                ).start()

            # Spin until someone gets the result, or they all finish without finding
            while len(list(filter((lambda x: x is True), finished))) < 2:
                pass

            found = len(list(filter((lambda x: x is True), finished))) >= 2
            if found:
                return True
            else:
                return False

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
            print("connected to {}".format(name))
        except TTransportException:
            print('Rescheduling connection attempt with ' + name + "@" + str(port))
            sleep(5)
            self.__attemptConnection(name, num, port)

    def __performReadRepair(self, key, val, ts):
    	excludedReplicasList = [3,0,1,2]
    	excludedReplica = excludedReplicasList[key//64]
    	
    	for rep_num in range(0,3,1):
    	    if rep_num == excludedReplica:
    	    	continue
    	    elif rep_num == self.number:
    	    	if val != self.store[key] and ts > self.time[key]:
    	    		self.store[key] = val
    	    		self.time[key] = ts
    	    else:
		self.establishedConnections[rep_num][0].put_aux(self.number, key, val, ts)
		
    def __dumpHintedHandoff(self, rep_num):
    	# Send all of our queued writes to this replica
    	for handoff in self.hintedHandoff[rep_num]:
    		self.establishedConnections[rep_num][0].put_aux(self.number, handoff[0], handoff[1], handoff[2])
    		
    	# The queue has been dumped, so we empty it here
    	self.hintedHandoff[rep_num] = []

def main():
    if len(sys.argv) != 4:
        print('Expected two arguments: name(str), port(int), consistency mechanism(int): 0 or 1 = read repair/hinted handoff')
        exit(-1)

    # Handle command-line arguments
    replicaName = sys.argv[1]
    replicaPort = int(sys.argv[2])
    consistencyMechanism = int(sys.argv[3])
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

    handler = StoreHandler(replicaName, replicaNumber, replicaConnectionsToMake, consistencyMechanism)
    processor = KeyValueStore.Processor(handler)
    transport = TSocket.TServerSocket(port=replicaPort)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    try:
        server.serve()
    except KeyboardInterrupt:
        handler.logFile.close()
        print('done.')

if __name__ == '__main__':
    main()
