import functools
import logging
import time
from operator import itemgetter

import psycopg2
from multiprocessing.dummy import RLock


def synchronized(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            logging.info('Entering in database operation ' + func.__name__)
            results = func(self, *args, **kwargs)
        return results

    return wrapper


class Database(object):
    PROFILES_FIELDS = ['owner_id', 'first_name', 'last_name', 'sex',
                       'screen_name', 'last_seen', 'bdate', 'verified',
                       'followers_count', 'country', 'city', 'processed']

    PHOTOS_FIELDS = ['owner_id', 'photo_id', 'likes', 'date',
                     'face_boundary', 'photo_path', 'photo_url', 'embedding']

    def __init__(self):
        self._conn = psycopg2.connect(user='postgres', password='password',
                                      database='users', host='localhost')
        self._conn.autocommit = True
        self._cursor = self._conn.cursor()
        self._lock = RLock()

    def __del__(self):
        self._conn.commit()
        self._cursor.close()
        self._conn.close()

    @synchronized
    def profiles_pagination(self, offset, limit, columns=None,
                            skip_processed_ids=False):
        if skip_processed_ids:
            self._cursor.execute(
                'SELECT * FROM profiles '
                'WHERE processed = FALSE '
                'LIMIT {limit} OFFSET {offset}'.format(
                    limit=limit, offset=offset
                )
            )
        else:
            self._cursor.execute(
                'SELECT * FROM profiles '
                'LIMIT {limit} OFFSET {offset}'.format(
                    limit=limit, offset=offset
                )
            )
        rows = self._cursor.fetchall()

        if columns is None:
            ret = rows
        else:
            ret = [itemgetter(*columns)(row) for row in rows]

        scanned_rows = self._cursor.rowcount
        return ret, scanned_rows

    @synchronized
    def remove_profiles(self, remove_ids):
        self._conn.commit()
        if len(remove_ids) == 0:
            return

        rows = []
        for owner_id in remove_ids:
            row = self._mogrify((owner_id,))
            rows.append(row.decode('utf-8'))

        self._cursor.execute(
            'DELETE FROM profiles '
            'WHERE owner_id IN (VALUES {remove_ids})'.format(
                remove_ids=','.join(rows)
            )
        )

    @synchronized
    def mark_processed_profiles(self, mark_ids):
        if len(mark_ids) == 0:
            return

        rows = []
        for owner_id in mark_ids:
            row = self._mogrify((owner_id,))
            rows.append(row.decode('utf-8'))

        self._cursor.execute(
            'UPDATE profiles SET processed = TRUE '
            'WHERE owner_id IN (VALUES {mark_ids})'.format(
                mark_ids=','.join(rows)
            )
        )

    @synchronized
    def get_empty_photos_embeddings(self, offset, limit):
        self._cursor.execute(
            'SELECT photo_id, photo_path FROM photos '
            'WHERE embedding = {0} '
            'LIMIT {limit} OFFSET {offset}'.format(
                limit=limit, offset=offset
            )
        )

        rows = self._cursor.fetchall()
        scanned_rows = self._cursor.rowcount
        return rows, scanned_rows

    @synchronized
    def update_embeddings(self, embeddings):
        embeddings = self._transform_input_data(embeddings)
        self._cursor.execute(
            'UPDATE photos AS photo SET '
            '    embedding = photo_new.embedding '
            'FROM (VALUES {embeddings}) '
            'AS photo_new(owner_id, embedding) '
            'WHERE photo_new.owner_id = photo.owner_id'.format(
                embeddings=','.join(embeddings)
            )
        )

    def _mogrify(self, params):
        return self._cursor.mogrify(
            '({})'.format(','.join(['%s'] * len(params))), params)

    def _transform_input_data(self, data):
        rows = []
        for row in data:
            row = self._mogrify(row)
            rows.append(row.decode('utf-8'))
        return rows

    @synchronized
    def insert_photos(self, rows):

        rows = self._transform_input_data(rows)
        start_time = time.time()
        self._cursor.execute(
            'WITH new_rows ({fields}) AS (VALUES {rows}) '
            'INSERT INTO photos ({fields}) '
            'SELECT {fields} '
            'FROM new_rows '
            'WHERE NOT EXISTS (SELECT photo_id FROM photos up '
            'WHERE up.photo_id=new_rows.photo_id)'.format(
                fields=u','.join(self.PHOTOS_FIELDS), rows=u','.join(rows)
            )
        )
        elapsed_time = time.time() - start_time
        logging.info('New profiles inserted in {} ms'
                     .format(int(elapsed_time * 1000)))

    @synchronized
    def insert_profiles(self, rows):
        rows = self._transform_input_data(rows)
        start_time = time.time()
        self._cursor.execute(
            'WITH new_rows ({fields}) AS (VALUES {rows}) '
            'INSERT INTO profiles ({fields}) '
            'SELECT {fields} '
            'FROM new_rows '
            'WHERE NOT EXISTS (SELECT owner_id FROM profiles up '
            'WHERE up.owner_id=new_rows.owner_id)'.format(
                fields=u','.join(self.PROFILES_FIELDS), rows=u','.join(rows)
            )
        )
        elapsed_time = time.time() - start_time
        logging.info('New profiles inserted in {} ms'
                     .format(int(elapsed_time * 1000)))
