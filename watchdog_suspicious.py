from ckanapi import RemoteCKAN
import requests_cache
import dataset
from datetime import timedelta, datetime

USER_AGENT = 'ckan-watchdog/0.1 (+https://github.com/ad-m/ckan-watchdog)'
CKAN_URL = 'https://danepubliczne.gov.pl/'

requests_cache.install_cache()


class Package(object):
    TIME_DIFF = {u'everyHalfYear': timedelta(30*6),
                 u'monthly': timedelta(30),
                 u'quarterly': timedelta(3*30),
                 u'weekly': timedelta(7),
                 u'yearly': timedelta(365),
                 u'daily': timedelta(1)}
    KNOWN_TIME = TIME_DIFF.keys() + ['notApplicable']
    BUFOR = timedelta(7)
    _modified_date = None

    def __init__(self, ckan, short_resp=None, long_resp=None):
        assert short_resp or long_resp
        self._short_resp = short_resp
        self._long_resp = long_resp
        self.ckan = ckan

    @property
    def short_resp(self):
        return self._long_resp or self._short_resp

    @property
    def long_resp(self):
        if not self._long_resp:
            self._long_resp = self.ckan.action.package_show(id=self._short_resp['id'])
        return self._long_resp

    def is_valid(self):
        if self.short_resp.get('api', False):
            print "Skip %s due API is %s" % (self.short_resp['id'],
                                             str(self.short_resp.get('api', 'unknown')))
            return False
        if not self.short_resp['resources']:
            print "Skip %s due %d resources" % (self.short_resp['id'],
                                                len(self.short_resp['resources']))
            return False
        if 'update_frequency' not in self.short_resp:
            print "Skip %s due lack of frequency info" % (self.short_resp['id'])
            return False
        if self.short_resp['update_frequency'] not in self.TIME_DIFF:
            print "Skip %s due unknown time diff '%s'" % (self.short_resp['id'],
                                                          self.short_resp['update_frequency'])
            return False
        return True

    def modified_date(self):
        if self._modified_date:
            return self._modified_date
        if not self.short_resp['resources']:
            modified = None
        else:
            modified = [z['last_modified'] or z['created'] for z in self.short_resp['resources']]
            modified = [x.split('.')[0] for x in modified]
            modified = map(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"), modified)
            modified = max(modified)
        self._modified_date = modified
        return modified

    def fresh_time(self):
        return self.modified_date() + self.TIME_DIFF[self.short_resp['update_frequency']]

    def is_current(self):
        fresh_time = self.fresh_time()
        return fresh_time < datetime.now() - self.BUFOR

    def any_uploaded(self):
        return any(x.get('url_type', None) == 'upload' for x in self.short_resp['resources'])

    def any_datastore(self):
        return any(x.get('datastore_active', False) for x in self.long_resp['resources'])

    def as_dict(self):
        return {'org_name': self.short_resp['organization']['title'],
                'org_id': self.short_resp['organization']['id'],
                'dataset_id': self.short_resp['id'],
                'dataset_name': self.short_resp['title'],
                'is_current': self.is_current(),
                'modified_date': self.modified_date(),
                'fresh_time': self.fresh_time(),
                'any_uploaded': self.any_uploaded(),
                'any_datastore': self.any_datastore(),
                'frequency': self.short_resp['update_frequency']}

    def id(self):
        return self.short_resp['id']


class Watchdog(object):
    def __init__(self, ckan=None):
        self.ckan = ckan or RemoteCKAN(CKAN_URL, user_agent=USER_AGENT, get_only=True)

    def packages_generator(self, per_page=100):
        offset = 0
        packages = []
        while offset == 0 or packages:
            packages = self.ckan.action.package_search(start=offset, rows=per_page)['results']
            for result in packages:
                yield Package(self.ckan, short_resp=result)
            offset += per_page

    def fill_table(self, table):
        for package in self.packages_generator():
            if not package.is_valid():
                continue
            print "Saved %s package" % (package.id(), )
            table.insert(package.as_dict())
        print "Ready to export %d rows" % (table.count(), )

    def run(self, filename, filetype='csv'):
        table = dataset.connect('sqlite:///:memory:')['any_table']
        self.fill_table(table)
        dataset.freeze(table.all(), format=filetype, filename=filename)


Watchdog().run("suspicious.csv")
