import logging

from database import Database
from detector import FaceDetector
from loaders.photos import PhotosLoader
# from loaders.profiles import ProfilesLoader
# from representer import FaceRepresenter
from storage import Storage

if __name__ == '__main__':
    logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(asctime)s '
                               u'%(levelname)-8s [%(threadName)s]  %(message)s',
                        level=logging.DEBUG, filename=u'app.logs', filemode='w')

    database = Database()
    storage = Storage()
    detector = FaceDetector()
    # representer = FaceRepresenter()
    # ProfilesLoader(database, storage).load()
    PhotosLoader(database, detector, None).start()
    # PhotosLoader(database, None, None).start()
    # ProfilesLoader(database, storage).start()
