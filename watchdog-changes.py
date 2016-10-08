from os.path import dirname, join
from os import environ
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pickledb
from ckanapi import RemoteCKAN


from jinja2 import Environment, FileSystemLoader
from lib.stores import DescriptionStore, ModifiedStore

USER_AGENT = 'ckan-watchdog/0.1 (+https://github.com/ad-m/ckan-watchdog)'
CKAN_URL = 'https://danepubliczne.gov.pl/'
ROOT = dirname(__file__)


def packages_generator(ckan, per_page=100):
    offset = 0
    packages = []
    while offset == 0 or packages:
        packages = ckan.action.package_search(start=offset, rows=per_page)['results']
        for result in packages:
            yield result
        offset += per_page


def get_fresh_resources(ckan, modified_store):
    resource_list = packages_generator(ckan)
    return filter(modified_store.if_fresh_resource, resource_list)


def get_diff_resources(fresh_resources, description_store):
    return map(description_store.diff, fresh_resources)


def get_content(**context):
    loader = FileSystemLoader(join(ROOT, 'templates'))
    environment = Environment(loader=loader, trim_blocks=True)
    template = environment.get_template('content.html.j2')
    return template.render(ckan_url=CKAN_URL,
                           **context).encode('utf-8')


def backup_message(msg):
    filename = datetime.now().strftime('%Y-%m-%d-%s.eml')
    filepath = join(join(ROOT, 'backups'), filename)
    open(filepath, 'wb').write(msg.as_string())


def main():
    ckan = RemoteCKAN(CKAN_URL, user_agent=USER_AGENT)
    store = pickledb.load(join(ROOT, 'data.db'), False)
    modified_store = ModifiedStore(store)
    description_store = DescriptionStore(store)

    fresh_resources = get_fresh_resources(ckan, modified_store)
    if not fresh_resources:
        sys.exit()
    diff_resources = get_diff_resources(fresh_resources, description_store)
    resources = zip(fresh_resources, diff_resources)
    content = get_content(resources=resources)

    user = environ['BOT_MAIL_USER']
    password = environ['BOT_MAIL_PASSWORD']
    dest_address = environ['BOT_DEST_ADDRESS'].split(',')
    host = environ['BOT_SERVER']

    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = dest_address[0]
    msg['Subject'] = datetime.now().strftime("Aktualizacje danepubliczne.gov.pl z %d-%m-%Y")

    msg.attach(MIMEText(content, 'html'))

    server = smtplib.SMTP(host, 25)
    if user and password:
        server.login(user, password)

    for dest in dest_address:
        server.sendmail(user, dest, msg.as_string())
    backup_message(msg)
    server.quit()
    map(modified_store.update, fresh_resources)
    map(description_store.update, fresh_resources)
    store.dump()


if __name__ == '__main__':
    main()
