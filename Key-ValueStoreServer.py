import os


class Store:
    def __init__(self, log_file=None):
        self.store = []
        self.log = None
        if log_file:
            for root, _, files in os.walk(""):
                for file in files:
                    if file == log_file:
                        self.log = open(file, 'r')
                        self.__populate_from_mem(self.log)
                if self.log is None:
                    self.log = open(log_file, 'rw+')

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
        if key in self.store:
            self.store[key] = value
            return True
        else:
            self.store[key] = value
            return False
