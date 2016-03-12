import logging
import time
from multiprocessing.dummy import RLock
from operator import itemgetter

import psycopg2

from decorators import synchronized


class Database:
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
                'ORDER BY owner_id '
                'LIMIT {limit} OFFSET {offset}'.format(
                    limit=limit, offset=offset
                )
            )
        else:
            self._cursor.execute(
                'SELECT * FROM profiles '
                'ORDER BY owner_id '
                'LIMIT {limit} OFFSET {offset}'.format(
                    limit=limit, offset=offset
                )
            )
        rows = self._cursor.fetchall()

        if columns is None:
            return rows
        else:
            return [itemgetter(*columns)(row) for row in rows]

    @synchronized
    def get_all_photos(self, columns=None):
        self._cursor.execute(
            'SELECT * FROM photos '
            'WHERE embedding != CAST(ARRAY[0] as double precision[]) '
            'ORDER BY photo_id'
        )
        rows = self._cursor.fetchall()

        if columns is None:
            return rows
        else:
            return [itemgetter(*columns)(row) for row in rows]

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
        logging.info('Deleted {} profiles'.format(self._cursor.rowcount))

    @synchronized
    def clean_wrong_photos(self):
        self._cursor.execute(
            'DELETE FROM photos photo WHERE '
            'NOT EXISTS (SELECT owner_id FROM profiles profile '
            'WHERE profile.owner_id=photo.owner_id)'
        )

        logging.info('Deleted {} photos'.format(self._cursor.rowcount))

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
    def get_photos_without_embeddings(self, limit):
        self._cursor.execute(
            'SELECT photo_id, photo_path FROM photos '
            'WHERE embedding = CAST(ARRAY[0] as double precision[]) '
            'LIMIT {limit}'.format(
                limit=limit
            )
        )

        rows = self._cursor.fetchall()
        return rows

    @synchronized
    def update_embeddings(self, embeddings):
        embeddings = self._transform_input_data(embeddings)
        self._cursor.execute(
            'UPDATE photos AS photo '
            'SET embedding = photo_new.embedding '
            'FROM (VALUES {embeddings}) '
            'AS photo_new(photo_id, embedding) '
            'WHERE photo_new.photo_id = photo.photo_id'.format(
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
