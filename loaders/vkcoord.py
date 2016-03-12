import time

from multiprocessing.dummy import RLock

VK_OATH2_URL = 'https://oauth.vk.com/authorize?client_id=5236630&display=page&' \
               'redirect_uri=http://fisko.me/callback&scope=offline,photos&response_type=token'
VK_ACCESS_TOKEN = 'RENEW_TOKEN'

VK_CODE = 'https://oauth.vk.com/authorize?client_id=5236630&display=page&redirect_uri=' \
          'https://tjournal.ru/callback&scope=photos,%20offline&response_type=code&v=5.45'

VK_SERVER_ACCESS_TOKEN = 'https://oauth.vk.com/access_token?client_id=5236630&client_secret' \
                         '=jIjD77zlvrN2AnmG0lh5&redirect_uri=https://tjournal.ru/callback&code=440ce6a102c9abdae5'

FACE_SAVE_DIR = '../../faces'


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
