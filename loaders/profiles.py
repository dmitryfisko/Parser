import json
from urllib2 import urlopen, HTTPError
import urlparse as urlparser
import urllib
from multiprocessing.dummy import Pool as ThreadPool

import time
from datetime import date as clock
import psycopg2

from loaders.vkcoord import VK_ACCESS_TOKEN, Coordinator


class ProfilesLoader(object):
    VK_SEARCH_API_URL = 'https://api.vk.com/method/users.search'
    CURR_YEAR = clock.today().year

    def _do_search(self, url, coord, cursor, age, month):
        wait = coord.next_wait_time()
        while wait != 0:
            time.sleep(wait)
            wait = coord.next_wait_time()

        try:
            response = urlopen(url).read()
            data = json.loads(response.decode('utf-8'))['response']
        except HTTPError:
            data = []
        except ValueError:
            data = []

        rows = []
        for profile in data[1:]:
            uid = profile['uid']
            first_name = profile['first_name']
            last_name = profile['last_name']
            sex = profile['sex']
            screen_name = profile['screen_name']
            last_seen = profile['last_seen']['time']

            try:
                bdate = profile['bdate']
                if len(bdate) < 8:
                    bdate += '.' + str(self.CURR_YEAR - age)
            except:
                bdate = '00.%d.%d' % (month, self.CURR_YEAR - age)

            verified = True if profile['verified'] == 1 else False
            followers_count = profile['followers_count']
            country = profile['country']
            city = profile['city']

            row = cursor.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', (
                uid, first_name, last_name, sex,
                screen_name, last_seen, bdate,
                verified, followers_count, country, city
            ))
            rows.append(row.decode('utf-8'))

        if len(rows) == 0:
            return 'failed %s %s' % (age, month)

        with coord.lock:
            fields = 'uid,first_name,last_name,sex,screen_name,last_seen,bdate,verified,followers_count,country,city'
            start_time = time.time()
            cursor.execute(
                u'WITH new_rows ({fields}) AS (VALUES {rows}) '
                u'INSERT INTO profiles ({fields}) '
                u'SELECT {fields} '
                u'FROM new_rows '
                u'WHERE NOT EXISTS (SELECT uid FROM profiles up WHERE up.uid=new_rows.uid)'.format(
                    fields=fields, rows=u','.join(rows)
                ))
            print("--- %s seconds ---" % (time.time() - start_time))

        return 'ok'

    def _do_search_recall(self, params):
        return self._do_search(*params)

    def _generate_url(self, age, month):
        params = {'age_from': age, 'age_to': age, 'sort': 0,
                  'birth_month': month, 'count': 1000,
                  'fields': 'bdate,screen_name,sex,verified,last_seen,followers_count,country,city',
                  'has_photo': 1, 'access_token': VK_ACCESS_TOKEN}

        url_parts = list(urlparser.urlparse(self.VK_SEARCH_API_URL))
        query = dict(urlparser.parse_qsl(url_parts[4]))
        query.update(params)

        url_parts[4] = urllib.urlencode(query)

        print(urlparser.urlunparse(url_parts))
        return urlparser.urlunparse(url_parts)

    def _get_search_iterator(self, coord, db_cursor):
        for age in range(65, 17, -1):
            for month in range(12, 0, -1):
                yield (self._generate_url(age, month), coord, db_cursor, age, month)

    def load(self):
        pool = ThreadPool(5)
        coord = Coordinator()
        conn = psycopg2.connect(user='postgres', password='password',
                                database='users', host='localhost')
        conn.autocommit = True
        cursor = conn.cursor()
        results = pool.map(self._do_search_recall, self._get_search_iterator(coord, cursor))

        for result in results:
            print(result)

        pool.close()
        pool.join()

        conn.commit()
        cursor.close()
        conn.close()
