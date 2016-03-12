import logging
from multiprocessing.dummy import RLock

import time

import sys

from loaders.photos import PhotosLoader
from loaders.profiles import ProfilesLoader


class Scheduler:
    MIN_TIME_FOR_RENEW = 10800  # in seconds = 3 hours
    MAX_SEARCH_FAILURES = PhotosLoader.WORKER_POOL_SIZE * 2

    def __init__(self, database, storage, detector, representer):
        self._database = database
        self._storage = storage
        self._detector = detector
        self._representer = representer
        self._profiles_loader = \
            ProfilesLoader(self._database, self._storage, self)
        self._photos_loader = \
            PhotosLoader(self._database, self._detector)


        self._lock = RLock()
        self._search_failures = 0

    def handle_search_failure(self):
        self._search_failures += 1

    def cancel_tasks(self, signal, frame):
        logging.warning('KeyboardInterrupt occurred, thread closing started...')
        self._profiles_loader.cancel_task()
        self._photos_loader.cancel_task()
        self._representer.cancel_task()
        sys.exit(0)

    def schedule(self):
        if self._search_failures > self.MAX_SEARCH_FAILURES:
            with self._lock:
                start_time = time.time()
                self._gather_photos_and_compute_embeddings()
                elapsed_time = time.time() - start_time

                if elapsed_time < self.MIN_TIME_FOR_RENEW:
                    sleep_time = self.MIN_TIME_FOR_RENEW - elapsed_time
                    logging.info('Scheduler going to sleep for {} seconds'.format(sleep_time))
                    time.sleep(sleep_time)

                self._search_failures = 0

    def _gather_photos_and_compute_embeddings(self):
        self._profiles_loader.cleanup_db()
        self._storage.save_state()

        logging.info('Photos loading started')
        self._photos_loader.start()
        logging.info('Photos loading started finished')

        logging.info('Filling empty embeddings')
        self._representer.fill_empty_embeddings(self._database)
        logging.info('Filling empty embeddings finished')

    def start(self):
        logging.info('Scheduler started work')
        self._profiles_loader.start()
        logging.info('Scheduler finished work')

        self._gather_photos_and_compute_embeddings()
