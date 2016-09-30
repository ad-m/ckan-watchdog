import os
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
ROOT = os.path.dirname(__file__)

def get_fresh_resources(ckan, modified_store):
    resource_list = ckan.action.package_search(sort='metadata_modified asc', rows=50)['results']
    return filter(modified_store.if_fresh_resource, resource_list)


def get_diff_resources(fresh_resources, description_store):
    return map(description_store.diff, fresh_resources)


def get_content(fresh_resources, diff_resources):
    loader = FileSystemLoader(os.path.join(ROOT, 'templates'))
    environment = Environment(loader=loader, trim_blocks=True)
    template = environment.get_template('content.html.j2')
    return template.render(resources=zip(fresh_resources, diff_resources),
                           ckan_url=CKAN_URL).encode('utf-8')


def backup_message(msg):
    filename = datetime.now().strftime('%Y-%m-%d-%s.eml')
    filepath = os.path.join(os.path.join(ROOT, 'backups'), filename)
    open(filepath, 'wb').write(msg.as_string())


def main():
    ckan = RemoteCKAN(CKAN_URL, user_agent=USER_AGENT)
    store = pickledb.load('data.db', False)
    modified_store = ModifiedStore(store)
    description_store = DescriptionStore(store)

    fresh_resources = get_fresh_resources(ckan, modified_store)
    if not fresh_resources:
        sys.exit()
    diff_resources = get_diff_resources(fresh_resources, description_store)
    content = get_content(fresh_resources, diff_resources)

    user = os.environ['BOT_MAIL_USER']
    password = os.environ['BOT_MAIL_PASSWORD']
    dest_address = os.environ['BOT_DEST_ADDRESS']
    host = os.environ['BOT_SERVER']

    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = dest_address
    msg['Subject'] = datetime.now().strftime("Aktualizacje danepubliczne.gov.pl z %d-%m-%Y")

    msg.attach(MIMEText(content, 'html'))

    server = smtplib.SMTP(host, 25)
    if user and password:
        server.login(user, password)

    server.sendmail(user, dest_address, msg.as_string())
    backup_message(msg)
    server.quit()
    map(modified_store.update, fresh_resources)
    map(description_store.update, fresh_resources)
    store.dump()


if __name__ == '__main__':
    main()
