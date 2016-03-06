import logging
from multiprocessing.dummy import RLock

import time

from loaders.photos import PhotosLoader
from loaders.profiles import ProfilesLoader


class Scheduler(object):
    MIN_TIME_FOR_RENEW = 10800  # in seconds = 3 hours
    MAX_SEARCH_FAILURES = PhotosLoader.WORKER_POOL_SIZE * 2

    def __init__(self, database, storage, detector, representer):
        self._database = database
        self._storage = storage
        self._detector = detector
        self._representer = representer

        self._lock = RLock()
        self._search_failures = 0

    def handle_search_failure(self):
        self._search_failures += 1

    def schedule(self):
        if self._search_failures > self.MAX_SEARCH_FAILURES:
            with self._lock:
                start_time = time.time()
                self._gather_photos_and_compute_emdeddings()
                elapsed_time = start_time - time.time()

                if elapsed_time < self.MIN_TIME_FOR_RENEW:
                    sleep_time = self.MIN_TIME_FOR_RENEW - elapsed_time
                    logging.info('Scheduler going to sleep for {} seconds'.format(sleep_time))
                    time.sleep(sleep_time)

                self._search_failures = 0

    def _gather_photos_and_compute_emdeddings(self):
        logging.info('Photos loading started')
        PhotosLoader(self._database, self._detector).start()
        logging.info('Photos loading started finished')

        logging.info('Filling empty embeddings')
        self._representer.fill_empty_embeddings(self._database)
        logging.info('Filling empty embeddings finished')

    def start(self):
        logging.info('Scheduler started work')
        ProfilesLoader(self._database, self._storage, self).start()
        logging.info('Scheduler finished work')

        self._gather_photos_and_compute_emdeddings()
