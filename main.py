import logging

from database import Database
from detector import FaceDetector
from loaders.photos import PhotosLoader
from loaders.profiles import ProfilesLoader
from representer import FaceRepresenter
from storage import Storage

if __name__ == '__main__':
    logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# '
                               u'%(levelname)-8s [%(threadName)s]  %(message)s',
                        level=logging.DEBUG, filename=u'app.logs', filemode='w')

    database = Database()
    storage = Storage()
    detector = FaceDetector()
    # representer = FaceRepresenter()
    # ProfilesLoader(database, storage).load()
    PhotosLoader(database, detector, None).start()
    # PhotosLoader(database, None, None).start()

    '''
    img = io.imread('imgs/durov.jpg')
    img2 = io.imread('imgs/durov2.jpg')
    img3 = io.imread('imgs/durov3.jpg')
    detector = FaceDetector()
    detector.detect(img)
    detector.detect(img2)
    detector.detect(img3, visualize=True) '''
