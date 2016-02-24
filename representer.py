import os
import numpy as np
import openface


class FaceRepresenter(object):
    NET_INPUT_DIM = 96
    MODEL_DIR = '../openface/models'

    def __init__(self):
        dlib_model_path = os.path.join(
            self.MODEL_DIR, 'dlib', 'shape_predictor_68_face_landmarks.dat')
        openface_model_path = os.path.join(
            self.MODEL_DIR, 'openface', 'nn4.small2.v1.t7')
        self._align = openface.AlignDlib(dlib_model_path)
        self._net = openface.TorchNeuralNet(openface_model_path, self.NET_INPUT_DIM)

    def represent(self, image, face_box):
        aligned_face = self._align.align(self.NET_INPUT_DIM, image, face_box,
                                         landmarkIndices=openface.AlignDlib.OUTER_EYES_AND_NOSE)
        if aligned_face is None:
            raise Exception('Unable to align image')
        return self._net.forward(aligned_face)

    @staticmethod
    def simularity(rep1, rep2):
        diff = rep1 - rep2
        return np.dot(diff, diff)

