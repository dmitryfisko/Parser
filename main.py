import logging

from database import Database
from detector import FaceDetector
from loaders.photos import PhotosLoader
from loaders.profiles import ProfilesLoader
# from representer import FaceRepresenter
from representer import FaceRepresenter
from storage import Storage

if __name__ == '__main__':
    logging.basicConfig(format='%(filename)s[LINE:%(lineno)d]# %(asctime)s '
                               '%(levelname)-8s [%(threadName)s]  %(message)s',
                        level=logging.DEBUG, filename='app.logs', filemode='w')

    storage = Storage()
    database = Database()
    detector = FaceDetector()

    representer = FaceRepresenter()
    # ProfilesLoader(database, storage).start()
    # PhotosLoader(database, detector, None).start()

    #representer.fill_empty_embeddings(database)
    representer.find_closest_face(database, detector)


    # storage = Storage()
    # representer = FaceRepresenter()
    # PhotosLoader(database, None, None).start()
    # ProfilesLoader(database, storage).start()
