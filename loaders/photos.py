import threading
import Queue
import json
from urllib2 import urlopen, HTTPError
import urlparse as urlparser
import urllib
from multiprocessing.dummy import Pool as ThreadPool

import psycopg2
import time

from loaders.vkcoord import Coordinator, VK_ACCESS_TOKEN


class PhotosLoader(object):
    USERS_PER_REQUEST = 24

    def __init__(self, face_detector):
        self._detector = face_detector

    class Consumer(threading.Thread):
        VK_PHOTOS_API_URL = 'https://api.vk.com/method/execute.getProfilePhotos'

        def __init__(self, queue, cursor, coord, face_detector, face_representer):
            threading.Thread.__init__(self)
            self._queue = queue
            self._cursor = cursor
            self._coord = coord
            self._detector = face_detector
            self._representer = face_representer

        class Photo(object):
            pass

        def _download_image(self, url):
            pass

        def _save_face(self, image, faces, owner_id, photo_id):
            return ''
            pass

        def _generate_url(self, user_ids):
            params = {'user_ids': ','.join(user_ids),
                      'access_token': VK_ACCESS_TOKEN}

            url_parts = list(urlparser.urlparse(self.VK_PHOTOS_API_URL))
            query = dict(urlparser.parse_qsl(url_parts[4]))
            query.update(params)

            url_parts[4] = urllib.urlencode(query)

            print(urlparser.urlunparse(url_parts))
            return urlparser.urlunparse(url_parts)

        def run(self):
            while True:
                user_ids = self._queue.get()
                if isinstance(user_ids, str) and user_ids == 'quit':
                    break

                try:
                    response = urlopen(self._generate_url(user_ids)).read()
                    users_photos = json.loads(response.decode('utf-8'))['response']
                except HTTPError:
                    users_photos = []
                except ValueError:
                    users_photos = []

                photos = []
                photos_urls = []
                for user_photos in users_photos:
                    for user_photo in user_photos:
                        photo = self.Photo()
                        photo.owner_id = user_photo['owner_id']
                        photo.photo_id = user_photo['photo_id']
                        photo.likes = user_photo['likes']
                        photo.date = user_photo['date']
                        photo.url = user_photo['photo']
                        photos.append(photo)

                        photos_urls.append(photo.url)

                pool = ThreadPool(20)
                images = pool.map(self._download_image, photos_urls)
                pool.close()
                pool.join()

                image_iter = iter(images)
                for photo in photos:
                    image = next(image_iter)
                    faces = self._detector.detect(next(image))
                    if len(faces) == 1:
                        photo.embedding = self._representer(image, faces)
                        photo.path = self._save_face(
                            image, faces, photo.owner_id, photo.photo_id)
                    else:
                        photo.embedding = None

                with self._coord:
                    fields = 'owner_id,photo_id,likes,date,embedding,photo_path,photo_url'
                    rows = []
                    for photo in photos:
                        row = self._cursor.mogrify('(%s,%s,%s,%s,%s,%s,%s)', (
                            photo.owner_id, photo.photo_id, photo.likes, photo.date,
                            photo.embedding, photo.path, photo.url
                        ))
                        rows.append(row.decode('utf-8'))
                    start_time = time.time()
                    self._cursor.execute(
                        u'WITH new_rows ({fields}) AS (VALUES {rows}) '
                        u'INSERT INTO profiles ({fields}) '
                        u'SELECT {fields} '
                        u'FROM new_rows '
                        u'WHERE NOT EXISTS (SELECT uid FROM profiles up WHERE up.uid=new_rows.uid)'.format(
                            fields=fields, rows=u','.join(rows)
                        ))
                    print("faces added --- %s seconds ---" % (time.time() - start_time))

            print 'Thread closed'

    def start(self):
        conn = psycopg2.connect(user='postgres', password='password',
                                database='users', host='localhost')
        conn.autocommit = True
        cursor = conn.cursor()

        coord = Coordinator()
        queue = Queue.Queue(maxsize=100)
        worker_threads = self._build_worker_pool(queue, 20, cursor, coord)

        offset = 0
        while True:
            uids = cursor.execute('SELECT uid FROM profiles LIMIT {limit} OFFSET {offset}'.format(
                limit=self.USERS_PER_REQUEST, offset=offset
            )).fetchall()

            if len(uids) == 0:
                break

            queue.put(uids)
            offset += self.USERS_PER_REQUEST

        for _ in worker_threads:
            queue.put('quit')
        for worker in worker_threads:
            worker.join()

    def _build_worker_pool(self, queue, size, cursor, coord):
        workers = []
        for _ in range(size):
            worker = self.Consumer(queue, cursor, coord, self._detector)
            worker.start()
            workers.append(worker)
        return workers
