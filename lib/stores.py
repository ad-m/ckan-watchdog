import collections
from difflib import HtmlDiff
from pprint import pformat


def convert(dictionary):
    """Recursively converts dictionary keys to strings."""
    if isinstance(dictionary, collections.Mapping):
        return {str(k): convert(v) for k, v in dictionary.iteritems()}
    elif isinstance(dictionary, str):
        return unicode(dictionary, errors='replace')
    return dictionary


class AbstractStore(object):
    prefix = "MODIFIED_"

    def __init__(self, store):
        self.store = store

    def key(self, resource):
        return self.prefix + resource['id']


class ModifiedStore(AbstractStore):
    prefix = "MODIFIED_"

    def if_fresh_resource(self, resource):
        return resource['metadata_modified'] != self.store.get(self.key(resource))

    def update(self, resource):
        self.store.set(self.key(resource), resource['metadata_modified'])


class DescriptionStore(AbstractStore):
    prefix = "DESCRIPTION_"

    def get(self, resource):
        return self.store.get(self.key(resource)) or "(empty)"

    def diff(self, resource):
        prev = self.get(resource)
        return HtmlDiff().make_table(pformat(convert(prev)).split("\n"),
                                     pformat(convert(resource)).split("\n"))

    def update(self, resource):
        self.store.set(self.key(resource), resource)
