import functools
import psycopg2
from multiprocessing.dummy import RLock


class Database(object):
    def __init__(self):
        self._conn = psycopg2.connect(user='postgres', password='password',
                                      database='users', host='localhost')
        self._conn.autocommit = True
        self._cursor = self._conn.cursor()
        self._lock = RLock()

    def synchronized(self, func):
        @functools.wraps(func)
        def new_func(*args, **kwargs):
            with self._lock:
                results = func(*args, **kwargs)
            return results

        return new_func

    @synchronized
    def profiles_pagination(self, limit, offset):
        self._cursor.execute('SELECT uid FROM profiles LIMIT {limit} OFFSET {offset}'.format(
            limit=limit, offset=offset
        ))
        return [user_id[0] for user_id in self._cursor.fetchall()]

    def mogrify(self, query, params):
        return self._cursor.mogrify(query, params)

    @synchronized
    def insert_photos(self, rows):
        fields = ['owner_id', 'photo_id', 'likes', 'date',
                  'face_boundary', 'photo_path', 'photo_url', 'embedding']
        assert len(rows) == len(fields)
        self._cursor.execute(
            u'WITH new_rows ({fields}) AS (VALUES {rows}) '
            u'INSERT INTO photos ({fields}) '
            u'SELECT {fields} '
            u'FROM new_rows '
            u'WHERE NOT EXISTS (SELECT photo_id FROM photos up WHERE up.photo_id=new_rows.photo_id)'.format(
                fields=fields, rows=u','.join(rows)
            ))

    @synchronized
    def insert_profiles(self, rows):
        fields = ['owner_id', 'first_name', 'last_name', 'sex',
                  'screen_name', 'last_seen', 'bdate', 'verified',
                  'followers_count', 'country', 'city']
        assert len(rows) == len(fields)
        self._cursor.execute(
            u'WITH new_rows ({fields}) AS (VALUES {rows}) '
            u'INSERT INTO profiles ({fields}) '
            u'SELECT {fields} '
            u'FROM new_rows '
            u'WHERE NOT EXISTS (SELECT owner_id FROM profiles up WHERE up.owner_id=new_rows.owner_id)'.format(
                fields=fields, rows=u','.join(rows)
            ))

    def __del__(self):
        self._conn.commit()
        self._cursor.close()
        self._conn.close()
