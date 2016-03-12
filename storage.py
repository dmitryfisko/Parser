import os
import pickle


class Storage:
    STORAGE_PATH = 'storage.dat'

    def __init__(self):
        self._container = self._load()
        if self._container is None:
            self._container = dict()

    def __getitem__(self, key):
        if key in self._container:
            return self._container[key]
        else:
            return []

    def add_to_key(self, key, value, value_type=None):
        if key not in self._container:
            if value_type is None:
                self._container[key] = value
            else:
                self._container[key] = value_type()
                self._container[key].add(value)
        else:
            self._container[key].add(value)

    def set(self, key, value):
        self._container[key] = value

    def remove(self, key):
        self._container[key] = None

    def _load(self):
        if os.path.exists(self.STORAGE_PATH):
            try:
                with open(self.STORAGE_PATH, 'rb') as f:
                    return pickle.load(f)
            except Exception:
                return None
        else:
            return None

    def save_state(self):
        with open(self.STORAGE_PATH, 'wb') as f:
            pickle.dump(self._container, f)
