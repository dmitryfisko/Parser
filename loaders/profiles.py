import httplib
import json
import logging
import time
import urllib
import urlparse as urlparser
from _socket import timeout
from datetime import date as clock
from multiprocessing.dummy import Pool as ThreadPool
from urllib2 import urlopen, HTTPError

from loaders.vkcoord import VK_ACCESS_TOKEN, VKCoordinator


class ProfilesLoader(object):
    VK_SEARCH_API_URL = 'https://api.vk.com/method/users.search'
    STORAGE_KEY = 'SEARCH_SUCCESS_REQUEST_PARAMS'
    CURR_YEAR = clock.today().year

    def __init__(self, database, storage):
        self._database = database
        self._storage = storage
        self._vk_coord = VKCoordinator()

    def _do_search(self, request_url, age, month):
        wait = self._vk_coord.next_wait_time()
        while wait != 0:
            time.sleep(wait)
            wait = self._vk_coord.next_wait_time()

        try:
            response = urlopen(request_url).read()
            data = json.loads(response.decode('utf-8'))['response']['items']
        except ValueError:
            logging.error(u"Profiles request failed "
                          u"to parse response of url: {}".format(request_url))
            data = []
        except HTTPError:
            logging.error("Profiles request HTTPException")
            data = []
        except httplib.HTTPException:
            logging.error("Profiles request HTTPException")
            data = []
        except timeout:
            logging.error("Profiles request timeout")
            data = []
        except Exception:
            logging.exception("Profiles request unknown exception")
            data = []

        rows = []
        for profile in data:
            owner_id = profile['id']
            first_name = profile['first_name']
            last_name = profile['last_name']
            sex = profile['sex']
            screen_name = profile['screen_name']
            last_seen = profile['last_seen']['time']

            try:
                bdate = profile['bdate']
                # string with len less than 8 haven't year
                if len(bdate) < 8:
                    bdate += '.' + str(self.CURR_YEAR - age)
            except Exception:
                bdate = '00.%d.%d' % (month, self.CURR_YEAR - age)

            verified = True if profile['verified'] == 1 else False
            followers_count = profile['followers_count']
            country = profile['country']['id'] if 'country' in profile else -1
            city = profile['city']['id'] if 'city' in profile else -1

            params = (
                owner_id, first_name, last_name, sex,
                screen_name, last_seen, bdate,
                verified, followers_count, country, city
            )
            row = self._database.mogrify(
                '({})'.format(','.join(['%s'] * len(params))), params)
            rows.append(row.decode('utf-8'))

        if len(rows) == 0:
            logging.warning(
                'Profiles got empty response for age {} month {}'.format(age, month))
            return

        self._database.insert_profiles(rows)
        self._storage.add_to_key(self.STORAGE_KEY, (age, month), value_type=set)

        logging.info('Profiles with age {} month {} processed'.format(age, month))

    def _do_search_recall(self, params):
        return self._do_search(*params)

    def _generate_url(self, age, month):
        params = {'age_from': age, 'age_to': age, 'sort': 0,
                  'birth_month': month, 'count': 1000, 'has_photo': 1,
                  'fields': 'bdate,screen_name,sex,verified,'
                            'last_seen,followers_count,country,city',
                  'v': '5.45', 'access_token': VK_ACCESS_TOKEN}

        url_parts = list(urlparser.urlparse(self.VK_SEARCH_API_URL))
        query = dict(urlparser.parse_qsl(url_parts[4]))
        query.update(params)

        url_parts[4] = urllib.urlencode(query)

        print(urlparser.urlunparse(url_parts))
        return urlparser.urlunparse(url_parts)

    def _get_search_iterator(self):
        # age param in search {65 ... 18}
        for age in xrange(65, 17, -1):
            # month param in search {12 ... 1}
            for month in xrange(12, 0, -1):
                if (age, month) in self._storage[self.STORAGE_KEY]:
                    continue
                yield (self._generate_url(age, month), age, month)

    def load(self):
        pool = ThreadPool(5)

        pool.map(self._do_search_recall,
                 self._get_search_iterator())

        pool.close()
        pool.join()
