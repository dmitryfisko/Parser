import json
import logging
import os
import re
import threading
import time
from multiprocessing.dummy import Pool as ThreadPool

from urllib import parse as urlparser
from urllib.request import urlopen
from urllib.parse import urlencode

from urllib.error import HTTPError
from _socket import timeout
from http.client import HTTPException

import numpy as np
from PIL import Image
from skimage import io

from loaders.vkcoord import VK_ACCESS_TOKEN, FACE_SAVE_DIR


class PhotosTask(threading.Thread):
    VK_PHOTOS_API_URL = 'https://api.vk.com/method/execute.getProfilePhotos'
    PHOTOS_REQUEST_TIMEOUT = 3
    IMAGE_REQUEST_TIMEOUT = 10
    IMAGES_LOADER_POOL_SIZE = 20

    def __init__(self, que, vk_coord, database, face_detector, face_representer):
        threading.Thread.__init__(self)
        self._queue = que
        self._vk_coord = vk_coord
        self._database = database
        self._detector = face_detector
        self._representer = face_representer

    class Photo(object):
        pass

    def _download_image(self, url):
        try:
            image = io.imread(url, timeout=self.IMAGE_REQUEST_TIMEOUT)
        except HTTPError:
            logging.warning('Image downloading HTTPError')
            return None
        except HTTPException:
            logging.warning('Image downloading HTTPException')
            return None
        except timeout:
            logging.warning('Image downloading timeout')
            return None
        except Exception:
            logging.exception('Image downloading unknown error')
            return None

        if len(image.shape) != 3 or (image.shape[2] not in [3, 4]):
            logging.warning('Downloaded image has wrong image depth')
            return None

        return image

    def _save_face(self, image, face, owner_id, photo_id):
        face = image[face.top():face.bottom(), face.left():face.right()]
        im = Image.fromarray(np.uint8(face))

        owner_id_norm = '{0:09d}'.format(owner_id)
        photo_id_norm = '{0:010d}'.format(photo_id)
        dirs = '/'.join(re.findall('...', owner_id_norm))
        directory = FACE_SAVE_DIR + '/' + dirs

        if not os.path.exists(directory):
            os.makedirs(directory)

        file_name = owner_id_norm + '_' + photo_id_norm + '.jpg'
        path = directory + '/' + file_name
        try:
            im.save(path)
        except Exception:
            logging.exception('image saving error')
            return None

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

        url_parts[4] = urlencode(query)

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

            logging.info('Next profiles processing in PhotosTask started')

            request_url = self._generate_url(user_ids)
            try:
                response = urlopen(request_url,
                                   timeout=self.PHOTOS_REQUEST_TIMEOUT).read()
                users_photos = json.loads(response.decode('utf-8'))['response']
            except ValueError:
                logging.error(u"Photos request failed "
                              u"to parse response of url: {}".format(request_url))
                users_photos = []
            except HTTPError:
                logging.error("Photos request HTTPException")
                users_photos = []
            except HTTPException:
                logging.error("Photos request HTTPException")
                users_photos = []
            except timeout:
                logging.error("Photos request timeout")
                users_photos = []
            except Exception:
                logging.exception("Photos request unknown exception")
                users_photos = []

            photos = []
            photos_urls = []
            owner_ids = []
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
                owner_ids.append(photos[-1].owner_id)

            pool = ThreadPool(self.IMAGES_LOADER_POOL_SIZE)
            images = pool.map(self._download_image, photos_urls)
            pool.close()
            pool.join()

            images_iter = iter(images)
            for photo in photos:
                image = next(images_iter)
                faces = self._detector.detect(image)
                if len(faces) == 1 and self._check_boundary(image, faces[0]):
                    face = faces[0]
                    photo.boundary = [face.top(), face.right(), face.bottom(), face.left()]
                    photo.path = self._save_face(
                        image, face, photo.owner_id, photo.photo_id)
                    if self._representer is not None:
                        photo.embedding = self._representer.represent(image, face)
                    else:
                        photo.embedding = np.array([0.])
                else:
                    photo.boundary = None

            rows = []
            for photo in photos:
                if photo.boundary is None:
                    continue

                row = (
                    photo.owner_id, photo.photo_id, photo.likes,
                    photo.date, photo.boundary, photo.path,
                    photo.url, photo.embedding.tolist()
                )
                rows.append(row)
            if len(rows) > 0:
                self._database.insert_photos(rows=rows)
                self._database.mark_processed_profiles(mark_ids=owner_ids)

            logging.info('Next profiles processing in PhotosTask ended')

        logging.info('PhotoTask thread close')
