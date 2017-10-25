import json
from multiprocessing import managers

from queue import Empty
import sys

class IOQueue:
    _sentinel = "$ioqueue_sentinel$"
    _buffer_size = 128  # elements.

    def __init__(self, filename):
        self._push_buffer = []
        self._pop_buffer = []
        self._buffer_idx = 0
        self._popped = 0
        self._pushed = 0
        self._filename = filename
        self._seek_to = 0

    def get_statistics(self):
        return self._popped, self._pushed

    def size(self):
        return self._pushed - self._popped

    def get(self):
        if self._buffer_idx >= len(self._pop_buffer):
            self._fill_pop_buffer()
        if len(self._pop_buffer) > self._buffer_idx:
            self._buffer_idx += 1
            self._popped += 1
            return self._pop_buffer[self._buffer_idx - 1]
        raise Empty

    def put(self, element):
        if len(self._push_buffer) >= IOQueue._buffer_size:
            self._flush_buffer()
        assert(len(self._push_buffer) < IOQueue._buffer_size)
        self._push_buffer.append(element)
        self._pushed += 1

    def _fill_pop_buffer(self):
        assert(self._buffer_idx >= len(self._pop_buffer))
        if self.size() < IOQueue._buffer_size:
            self._pop_buffer = [x for x in self._push_buffer]
            self._push_buffer = []
            self._buffer_idx = 0
            return

        with open(self._filename) as qfile:
            qfile.seek(self._seek_to)
            contents = ""
            while True:
                contents += qfile.read(2**16)
                if contents.find(IOQueue._sentinel):
                    break

            idx = contents.find(IOQueue._sentinel)
            self._seek_to += idx + len(IOQueue._sentinel)
            contents = contents[:idx]

            try:
                self._pop_buffer = [x for x in json.loads(contents)]
            except json.JSONDecodeError as e:
                print(contents, file=sys.stderr)
                raise
            self._pop_buffer = [tuple(x) if type(x) == list else x
                                 for x in self._pop_buffer]
            self._buffer_idx = 0
            return

    def _flush_buffer(self):
        assert(len(self._push_buffer) > 0)
        with open(self._filename, 'a') as qfile:
            to_write = json.dumps(self._push_buffer)
            print(to_write, file=qfile, end='')
            print(IOQueue._sentinel, file=qfile, end='')
            self._push_buffer = []


class IOQueueManager(managers.BaseManager):
    pass


IOQueueManager.register('IOQueue', IOQueue)
