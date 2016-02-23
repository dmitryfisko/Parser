from skimage import io

from detector import FaceDetector
from loaders.profiles import ProfilesLoader

if __name__ == '__main__':
    ProfilesLoader().load()

    '''
    img = io.imread('imgs/durov.jpg')
    img2 = io.imread('imgs/durov2.jpg')
    img3 = io.imread('imgs/durov3.jpg')
    detector = FaceDetector()
    detector.detect(img)
    detector.detect(img2)
    detector.detect(img3, visualize=True) '''


