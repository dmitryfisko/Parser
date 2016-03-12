import queue

from loaders.photostask import PhotosTask
from loaders.vkcoord import VKCoordinator


class PhotosLoader:
    USERS_PER_DB_REQUEST = 24
    QUEUE_MAX_SIZE = 100
    WORKER_POOL_SIZE = 10

    def __init__(self, database, face_detector):
        self._database = database
        self._detector = face_detector
        self._vk_coord = VKCoordinator()
        self._task_canceled = False

    def cancel_task(self):
        self._task_canceled = True

    def start(self):
        que = queue.Queue(maxsize=self.QUEUE_MAX_SIZE)
        worker_threads = self._build_worker_pool(que, self.WORKER_POOL_SIZE)

        offset = 0
        while True and not self._task_canceled:
            user_ids = self._database.profiles_pagination(
                offset=offset, limit=self.USERS_PER_DB_REQUEST,
                skip_processed_ids=True, columns=[0])

            if len(user_ids) == 0:
                break

            que.put(user_ids)
            offset += len(user_ids)

        for _ in worker_threads:
            que.put('quit')
        for worker in worker_threads:
            worker.join()

    def _build_worker_pool(self, que, size):
        workers = []
        for _ in range(size):
            worker = PhotosTask(que, self._vk_coord,
                                self._database, self._detector)
            worker.start()
            workers.append(worker)
        return workers
