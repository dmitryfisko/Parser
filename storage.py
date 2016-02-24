import os
import pickle


class Storage(object):
    def __init__(self):
        self._container = dict()
        self._STORAGE_PATH = 'storage.dat'

    def __getitem__(self, key):
        return self._container[key]

    def set(self, key, value):
        self._container[key] = value

    def remove(self, key):
        self._container[key] = None

    def load(self):
        if os.path.exists(self._STORAGE_PATH):
            try:
                with open(self._STORAGE_PATH, 'rb') as f:
                    self._container = pickle.load(f)
            except:
                raise ValueError('Bad data file schema')
        else:
            raise IOError('File not exist')

    def save(self):
        with open(self._STORAGE_PATH, 'wb') as f:
            pickle.dump(self._container, f)