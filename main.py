import logging

from database import Database
from detector import FaceDetector
from representer import FaceRepresenter
from scheduler import Scheduler
from storage import Storage

if __name__ == '__main__':
    logging.basicConfig(format='%(filename)s[LINE:%(lineno)d]# %(asctime)s '
                               '%(levelname)-8s [%(threadName)s]  %(message)s',
                        level=logging.DEBUG, filename='app.logs', filemode='w')

    storage = Storage()
    database = Database()
    detector = FaceDetector()
    representer = FaceRepresenter()

    representer.find_closest_face(database, detector)

    scheduler = Scheduler(database, storage, detector, representer)
    scheduler.start()

