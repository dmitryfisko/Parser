import time

from multiprocessing.dummy import RLock


VK_OATH2_URL = 'https://oauth.vk.com/authorize?client_id=5236630&display=page&' \
               'redirect_uri=http://fisko.me/callback&scope=offline,photos&response_type=token'
VK_ACCESS_TOKEN = '11a912bc7ddc439822f562f177cf6e6365fcbe3cc2d99f22d2567dbfa9e65569a02d1409dbc85f80f85f2'


class VKCoordinator:
    def __init__(self):
        self._prev_time = 0
        self._vk_requests_delay = 1
        self.lock = RLock()

    def next_wait_time(self):
        with self.lock:
            curr_time = time.time()
            passed_time = curr_time - self._prev_time

            if passed_time >= self._vk_requests_delay:
                self._prev_time = curr_time
                return 0
            else:
                return self._vk_requests_delay - passed_time
