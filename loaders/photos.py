import Queue

from loaders.photostask import PhotosTask
from loaders.vkcoord import VKCoordinator


class PhotosLoader(object):
    USERS_PER_REQUEST = 24

    def __init__(self, database, face_detector, face_representer):
        self._database = database
        self._representer = face_representer
        self._detector = face_detector
        self._vk_coord = VKCoordinator()

    def start(self):
        queue = Queue.Queue(maxsize=100)
        worker_threads = self._build_worker_pool(queue, 5)

        offset = 0
        while True:
            user_ids = self._database.profiles_pagination(
                limit=self.USERS_PER_REQUEST, offset=offset)

            if len(user_ids) == 0:
                break

            queue.put(user_ids)
            offset += self.USERS_PER_REQUEST

        for _ in worker_threads:
            queue.put('quit')
        for worker in worker_threads:
            worker.join()

    def _build_worker_pool(self, queue, size):
        workers = []
        for _ in range(size):
            worker = PhotosTask(queue, self._vk_coord, self._database,
                                self._detector, self._representer)
            worker.start()
            workers.append(worker)
        return workers
