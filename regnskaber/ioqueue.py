import json
from multiprocessing import managers

from queue import Empty, Full

class IOQueue:
    __sentinel = "$ioqueue_sentinel$"
    __buffer_size = 128  # elements.

    def __init__(self, filename):
        self.__push_buffer = []
        self.__pop_buffer = []
        self.__buffer_idx = 0
        self.__popped = 0
        self.__pushed = 0
        self.__filename = filename
        self.__seek_to = 0

    def get_statistics(self):
        return self.__popped, self.__pushed

    def size(self):
        return self.__pushed - self.__popped

    def get(self):
        if self.__buffer_idx >= len(self.__pop_buffer):
            self.__fill_pop_buffer()
        if len(self.__pop_buffer) > self.__buffer_idx:
            self.__buffer_idx += 1
            self.__popped += 1
            return self.__pop_buffer[self.__buffer_idx - 1]
        raise Empty

    def put(self, element):
        if len(self.__push_buffer) >= IOQueue.__buffer_size:
            self.__flush_buffer()
        assert(len(self.__push_buffer) < IOQueue.__buffer_size)
        self.__push_buffer.append(element)
        self.__pushed += 1

    def __fill_pop_buffer(self):
        assert(self.__buffer_idx >= len(self.__pop_buffer))
        if self.size() < IOQueue.__buffer_size:
            self.__pop_buffer = [x for x in self.__push_buffer]
            self.__push_buffer = []
            self.__buffer_idx = 0
            return

        with open(self.__filename) as qfile:
            qfile.seek(self.__seek_to)
            contents = ""
            read = 0
            while True:
                contents += qfile.read(2**16)
                if contents.find(IOQueue.__sentinel):
                    break

            idx = contents.find(IOQueue.__sentinel)
            self.__seek_to += idx + len(IOQueue.__sentinel)
            contents = contents[:idx]

            self.__pop_buffer = [x for x in json.loads(contents)]
            self.__pop_buffer = [tuple(x) if type(x) == list else x for x in self.__pop_buffer]
            self.__buffer_idx = 0
            return

    def __flush_buffer(self):
        assert(len(self.__push_buffer) > 0)
        with open(self.__filename, 'a') as qfile:
            to_write = json.dumps(self.__push_buffer)
            print(to_write, file=qfile, end='')
            print(IOQueue.__sentinel, file=qfile, end='')
            self.__push_buffer = []

class IOQueueManager(managers.BaseManager):
    pass

IOQueueManager.register('IOQueue', IOQueue)
