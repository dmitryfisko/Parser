import time

from multiprocessing.dummy import RLock


VK_OATH2_URL = 'https://oauth.vk.com/authorize?client_id=5236630&display=page&' \
               'redirect_uri=http://fisko.me/callback&scope=offline&response_type=token'
VK_ACCESS_TOKEN = 'bcc4ac7458aa17e37a3fea08102d86afe83875cdd89338c4efb22c6341257d302648583adbd55bcad9352'


class Coordinator:
    def __init__(self):
        self._prev_time = 0
        self._request_delay = 0.4
        self.lock = RLock()

    def next_wait_time(self):
        with self.lock:
            curr_time = time.time()
            passed_time = curr_time - self._prev_time

            if passed_time >= self._request_delay:
                self._prev_time = curr_time
                return 0
            else:
                return self._request_delay - passed_time
