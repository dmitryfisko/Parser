import json
import urllib.request as urllib
from urllib import parse as urlparser
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing.dummy import RLock

import time
from datetime import date as clock
import psycopg2

VK_OATH2_URL = 'https://oauth.vk.com/authorize?client_id=5236630&display=page&' \
               'redirect_uri=http://fisko.me/callback&scope=offline&response_type=token'
VK_SEARCH_API_URL = 'https://api.vk.com/method/users.search'
VK_ACCESS_TOKE = 'bcc4ac7458aa17e37a3fea08102d86afe83875cdd89338c4efb22c6341257d302648583adbd55bcad9352'
CURR_YEAR = clock.today().year


class Coordinator:
  def __init__(self):
    self._prev_time = 0
    self._request_delay = 0.4
    self._lock = RLock()

  def next_wait_time(self):
    with self._lock:
      curr_time = time.time()
      passed_time = curr_time - self._prev_time

      if passed_time >= self._request_delay:
        self._prev_time = curr_time
        return 0
      else:
        return self._request_delay - passed_time


def do_search(url, coord, cursor, age, month):
  wait = coord.next_wait_time()
  while wait != 0:
    time.sleep(wait)
    wait = coord.next_wait_time()

  response = urllib.urlopen(url).read()
  data = json.loads(response.decode('utf-8'))['response']

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
        bdate += '.' + str(CURR_YEAR - age)
    except:
      bdate = '00.%d.%d' % (month, CURR_YEAR - age)

    verified = True if profile['verified'] == 1 else False
    followers_count = profile['followers_count']
    country = profile['country']
    city = profile['city']

    row = cursor.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                         (uid, first_name, last_name, sex,
                          screen_name, last_seen, bdate,
                          verified, followers_count, country, city))
    rows.append(row.decode('utf-8'))

  if len(rows) == 0:
    return 'failed %s %s' % (age, month)


  # start_time = time.time()
  # cursor.execute('INSERT INTO profiles VALUES %s' % (','.join(rows)))
  # print("--- %s seconds ---" % (time.time() - start_time))

  # cursor.execute('WITH new_values (uid, first_name, last_name, sex, screen_name, last_seen, bdate, verified, followers_count, country, city) AS (VALUES %s) '
  #                'INSERT INTO profiles (uid, first_name, last_name, sex, screen_name, last_seen, bdate, verified, followers_count, country, city) '
  #                'SELECT uid, first_name, last_name, sex, screen_name, last_seen, bdate, verified, followers_count, country, city '
  #                'FROM new_values '
  #                'WHERE NOT EXISTS (SELECT uid FROM profiles WHERE uid=new_values.uid)' % (','.join(rows)))

  with coord._lock:
    fields = 'uid,first_name,last_name,sex,screen_name,last_seen,bdate,verified,followers_count,country,city'
    start_time = time.time()
    cursor.execute(
      'WITH new_rows (%s) AS (VALUES %s) '
      'INSERT INTO profiles (%s) '
      'SELECT %s '
      'FROM new_rows '
      'WHERE NOT EXISTS (SELECT uid FROM profiles up WHERE up.uid=new_rows.uid)' % (fields, ','.join(rows), fields, fields))
    print("--- %s seconds ---" % (time.time() - start_time))

  # start_time = time.time()
  # with coord._lock:
  #  cursor.executemany("INSERT INTO profiles SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s"
  #                     " WHERE NOT EXISTS (SELECT uid FROM profiles WHERE uid=%s)", rows)
  # print("--- %s seconds ---" % (time.time() - start_time))

  return 'ok'


def generate_url(age, month):
  params = {'age_from': age, 'age_to': age, 'sort': 0,
            'birth_month': month, 'count': 1000,
            'fields': 'bdate,screen_name,sex,verified,relation,last_seen,followers_count,country,city',
            'has_photo': 1, 'access_token': VK_ACCESS_TOKE}

  url_parts = list(urlparser.urlparse(VK_SEARCH_API_URL))
  query = dict(urlparser.parse_qsl(url_parts[4]))
  query.update(params)

  url_parts[4] = urlparser.urlencode(query)

  print(urlparser.urlunparse(url_parts))
  return urlparser.urlunparse(url_parts)


def get_url_iterator(coord, db_cursor):
  for age in range(30, 17, -1):
    for month in range(12, 0, -1):
      yield (generate_url(age, month), coord, db_cursor, age, month)


def get_users_profiles():
  pool = ThreadPool(4)
  coord = Coordinator()
  conn = psycopg2.connect(user='postgres', password='password',
                          database='users', host='localhost')
  cursor = conn.cursor()
  results = pool.starmap(do_search, get_url_iterator(coord, cursor))

  for result in results:
    print(result)

  pool.close()
  pool.join()

  conn.commit()
  cursor.close()
  conn.close()


if __name__ == '__main__':
  start_time = time.time()
  get_users_profiles()
  print("--- %s seconds ---" % (time.time() - start_time))
