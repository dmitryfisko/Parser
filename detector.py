import dlib
import functools
import time
from multiprocessing.dummy import RLock


def timeit(func):
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        start_time = time.time()
        results = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        skip_param = 'visualize'
        if skip_param not in kwargs or not kwargs['visualize']:
            print('function [{}] finished in {} ms'.format(
                func.__name__, int(elapsed_time * 1000)))
        return results
    return new_func


class FaceDetector(object):
    def __init__(self):
        self._detector = dlib.get_frontal_face_detector()
        pose_predictor_path = '../dlib/models/shape_predictor_68_face_landmarks.dat'
        self._predictor = dlib.shape_predictor(pose_predictor_path)
        self._win = dlib.image_window()
        self._lock = RLock()


    @timeit
    def detect(self, image, landmarks=False, visualize=False):
        # img = rescale(image, preserve_range=True).astype(np.uint8)
        # The 1 in the second argument indicates that we should upsample the image
        # 1 time.  This will make everything bigger and allow us to detect more
        # faces.
        if image is None:
            return []

        with self._lock:
            dets, scores, idx = self._detector.run(image)
            poses = []
            if landmarks:
                for det in dets:
                    poses.append(self._predictor(image, det))
            if visualize:
                self._visualize(image, dets, poses, scores)

        if landmarks:
            return dets, poses
        else:
            return dets

    def _visualize(self, image, dets, poses, scores):
        print("Number of faces detected: {}".format(len(dets)))
        for i, d in enumerate(dets):
            print("Detection {} score: {:.2f}: Left: {} Top: {} Right: {} Bottom: {}".format(
                i, scores[i], d.left(), d.top(), d.right(), d.bottom()))

        self._win.clear_overlay()
        self._win.set_image(image)
        for pose in poses:
            self._win.add_overlay(pose)
        self._win.add_overlay(dets)
        dlib.hit_enter_to_continue()
