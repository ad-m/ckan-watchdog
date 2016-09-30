from pprint import pformat
from difflib import HtmlDiff


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
        return HtmlDiff().make_table(pformat(prev).split("\n"),
                                     pformat(resource).split("\n"))

    def update(self, resource):
        self.store.set(self.key(resource), resource)
