import os
from skimage import io

import numpy as np
import dlib
import openface
from multiprocessing.dummy import RLock
from multiprocessing.dummy import Pool as ThreadPool

from decorators import timeit
from loaders.vkcoord import FACE_SAVE_DIR


class FaceRepresenter(object):
    NET_INPUT_DIM = 96
    MODEL_DIR = '../openface/models'
    USERS_PER_DB_REQUEST = 10

    def __init__(self):
        dlib_model_path = os.path.join(
            self.MODEL_DIR, 'dlib', 'shape_predictor_68_face_landmarks.dat')
        openface_model_path = os.path.join(
            self.MODEL_DIR, 'openface', 'nn4.small2.v1.t7')
        self._align = openface.AlignDlib(dlib_model_path)
        self._net = openface.TorchNeuralNet(openface_model_path, self.NET_INPUT_DIM)
        self._lock = RLock()

    @staticmethod
    def simularity(rep1, rep2):
        diff = rep1 - rep2
        return np.dot(diff, diff)

    def represent(self, image, face_box):
        with self._lock:
            aligned_face = self._align.align(self.NET_INPUT_DIM, image, face_box,
                                             landmarkIndices=openface.AlignDlib.OUTER_EYES_AND_NOSE)
            if aligned_face is None:
                raise Exception('Unable to align image')
            return self._net.forward(aligned_face)

    @timeit
    def represent_image(self, item):
        face_path = item[1]
        face = io.imread(FACE_SAVE_DIR + '/' + face_path)
        height = face.shape[0]
        width = face.shape[1]
        face_box = dlib.rectangle(left=0, top=0, right=width - 1, bottom=height - 1)

        embedding = self.represent(face, face_box).tolist()
        owner_id = item[0]
        return owner_id, embedding

    def fill_empty_embeddings(self, database):
        pool = ThreadPool(5)

        while True:
            data = database.get_photos_without_embeddings(
                limit=self.USERS_PER_DB_REQUEST)

            if len(data) == 0:
                break

            embeddings = pool.map(self.represent_image, data)
            database.update_embeddings(embeddings)

        pool.close()
        pool.join()
