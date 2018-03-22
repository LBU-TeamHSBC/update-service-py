#!/usr/bin/env python3

import mysql.connector
from config import config
from adapters import GitHubAdapter

from pprint import pprint

class UpdateService(object):
    LINKED_ACCOUNTS_SQL = """SELECT sv.student_id, sv.oauth_token, v.id AS vendor_id, v.category
        FROM student_vendor AS sv
        INNER JOIN student s ON s.id=sv.student_id
        INNER JOIN user u ON u.id=s.user_id
        INNER JOIN vendor v ON v.id=sv.vendor_id"""

    SERVICE_MAP = {
        1: GitHubAdapter,
        # 2: UdemyAdapter,
        # 3: UdacityAdapter,
        # 4: LbuAdapter
    }

    SERVICE_TYPE_MAP = {
        'project': 'PROJECT',
        'course': 'COURSE'
    }
    
    def run(self):
        try:
            con = mysql.connector.connect(**config['db'])
            cursor = con.cursor()
            cursor.execute(UpdateService.LINKED_ACCOUNTS_SQL)
            for (uid, oauth, vid, cat) in cursor:
                AdapterClass = UpdateService.SERVICE_MAP[vid]
                data = AdapterClass(uid, oauth).getData()
                if cat == UpdateService.SERVICE_TYPE_MAP['project']:
                    for project in data:
                        print(project)
                else:
                    print('course')
        except mysql.connector.Error as err:
            print(err)
        else:
            con.close()

updateService = UpdateService()
updateService.run()