import functools
import logging
import time

import psycopg2
from multiprocessing.dummy import RLock


def synchronized(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            results = func(self, *args, **kwargs)
        return results

    return wrapper


class Database(object):
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
    def profiles_pagination(self, limit, offset):
        self._cursor.execute(
            u'SELECT owner_id FROM profiles profile_id '
            u'WHERE NOT EXISTS (SELECT owner_id FROM photos up '
            u'WHERE up.owner_id=profile_id.owner_id) '
            u'LIMIT {limit} OFFSET {offset}'.format(
                limit=limit, offset=offset
            ))
        return [user_id[0] for user_id in self._cursor.fetchall()]

    def mogrify(self, query, params):
        return self._cursor.mogrify(query, params)

    @synchronized
    def insert_photos(self, rows):
        fields = ['owner_id', 'photo_id', 'likes', 'date',
                  'face_boundary', 'photo_path', 'photo_url', 'embedding']
        start_time = time.time()
        self._cursor.execute(
            u'WITH new_rows ({fields}) AS (VALUES {rows}) '
            u'INSERT INTO photos ({fields}) '
            u'SELECT {fields} '
            u'FROM new_rows '
            u'WHERE NOT EXISTS (SELECT photo_id FROM photos up '
            u'WHERE up.photo_id=new_rows.photo_id)'.format(
                fields=u','.join(fields), rows=u','.join(rows)
            ))
        elapsed_time = time.time() - start_time
        logging.info('New profiles inserted in {} ms'
                     .format(int(elapsed_time * 1000)))

    @synchronized
    def insert_profiles(self, rows):
        fields = ['owner_id', 'first_name', 'last_name', 'sex',
                  'screen_name', 'last_seen', 'bdate', 'verified',
                  'followers_count', 'country', 'city']
        start_time = time.time()
        self._cursor.execute(
            u'WITH new_rows ({fields}) AS (VALUES {rows}) '
            u'INSERT INTO profiles ({fields}) '
            u'SELECT {fields} '
            u'FROM new_rows '
            u'WHERE NOT EXISTS (SELECT owner_id FROM profiles up '
            u'WHERE up.owner_id=new_rows.owner_id)'.format(
                fields=u','.join(fields), rows=u','.join(rows)
            ))
        elapsed_time = time.time() - start_time
        logging.info('New profiles inserted in {} ms'
                     .format(int(elapsed_time * 1000)))
