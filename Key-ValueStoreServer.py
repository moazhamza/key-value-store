class Store:
    def __init__(self):
        self.store = []
        # TODO: Look for your logging file, populate the memory if it exists, create one if it does not

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
