import httplib
import json
import os
import re
import threading
import time
import urllib
import urlparse as urlparser
from _socket import timeout
from multiprocessing.dummy import Pool as ThreadPool
from urllib2 import urlopen, HTTPError

import numpy as np
from PIL import Image
from skimage import io

from loaders.vkcoord import VK_ACCESS_TOKEN


class PhotosTask(threading.Thread):
    VK_PHOTOS_API_URL = 'https://api.vk.com/method/execute.getProfilePhotos'
    FACE_SAVE_PATH = '/home/user/faces'

    def __init__(self, queue, vk_coord, database, face_detector, face_representer):
        threading.Thread.__init__(self)
        self._queue = queue
        self._vk_coord = vk_coord
        self._database = database
        self._detector = face_detector
        self._representer = face_representer

    class Photo(object):
        pass

    @staticmethod
    def _download_image(url):
        try:
            image = io.imread(url, timeout=3)
        except HTTPError:
            print ('Image downloading HTTPError')
            return None
        except httplib.HTTPException:
            print ('Image downloading HTTPException')
            return None
        except timeout:
            print ('Image downloading timeout')
            return None
        except Exception:
            print ('Image downloading error')
            return None

        if len(image.shape) != 3 or (image.shape[2] not in [3, 4]):
            print ('Wrong image color type')
            return None

        return image

    def _save_face(self, image, face, owner_id, photo_id):
        face = image[face.top():face.bottom(), face.left():face.right()]
        im = Image.fromarray(np.uint8(face))

        owner_id_norm = '{0:09d}'.format(owner_id)
        photo_id_norm = '{0:010d}'.format(photo_id)
        dirs = '/'.join(re.findall('...', owner_id_norm))
        directory = self.FACE_SAVE_PATH + '/' + dirs

        if not os.path.exists(directory):
            os.makedirs(directory)

        file_name = owner_id_norm + '_' + photo_id_norm + '.jpg'
        path = directory + '/' + file_name
        im.save(path)

        return dirs + '/' + file_name

    @staticmethod
    def _check_boundary(image, face):
        if face.top() < 0 or face.left() < 0:
            return False
        height, width, depth = image.shape
        if face.bottom() >= height or face.right() >= width:
            return False

        return True

    def _generate_url(self, user_ids):
        user_ids = [str(user_id) for user_id in user_ids]
        params = {'user_ids': ','.join(user_ids), 'v': '5.45',
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
            wait = self._vk_coord.next_wait_time()
            while wait != 0:
                time.sleep(wait)
                wait = self._vk_coord.next_wait_time()

            try:
                response = urlopen(self._generate_url(user_ids), timeout=3).read()
                users_photos = json.loads(response.decode('utf-8'))['response']
            except HTTPError:
                users_photos = []
            except httplib.HTTPException:
                users_photos = []
            except timeout:
                users_photos = []
            except Exception:
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

            images_iter = iter(images)
            for photo in photos:
                image = next(images_iter)
                faces = self._detector.detect(image)
                if len(faces) == 1 and self._check_boundary(image, faces[0]):
                    face = faces[0]
                    photo.embedding = self._representer.represent(image, face)
                    photo.boundary = [face.top, face.right, face.bottom, face.left]
                    photo.path = self._save_face(
                        image, face, photo.owner_id, photo.photo_id)
                else:
                    photo.embedding = None

            rows = []
            for photo in photos:
                if photo.embedding is None:
                    continue
                params = (
                    photo.owner_id, photo.photo_id, photo.likes,
                    photo.date, photo.boundary, photo.path,
                    photo.url, photo.embedding.tolist()
                )
                row = self._database.mogrify(
                    '({})'.format(','.join(['%s'] * len(params))), params)
                rows.append(row.decode('utf-8'))
            if len(rows) > 0:
                start_time = time.time()
                self._database.insert_photos(rows=rows)
                print("faces added --- %s seconds ---" % (time.time() - start_time))

        print 'Thread closed'
