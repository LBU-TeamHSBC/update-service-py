#!/usr/bin/env python3

import mysql.connector
from time import sleep

from config import config
from adapters import GitHubAdapter, LbuAdapter
from processors import CourseProcessor, ProjectProcessor

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
        4: LbuAdapter
    }

    SERVICE_TYPE_MAP = {
        'project': 'PROJECT',
        'course': 'COURSE'
    }
    
    def run(self):
        while True:
            print("Fetching data:")
            try:
                db = mysql.connector.connect(**config['db'])
                
                projProcessor = ProjectProcessor(db)
                corsProcessor = CourseProcessor(db)

                cursor = db.cursor(buffered=True)
                cursor.execute(UpdateService.LINKED_ACCOUNTS_SQL)
                for (sid, oauth, vid, cat) in cursor:
                    print("  ", sid, vid, cat)
                    AdapterClass = UpdateService.SERVICE_MAP[vid]
                    data = AdapterClass(sid, oauth).getData()
                    if cat == UpdateService.SERVICE_TYPE_MAP['project']:
                        for project in data:
                            projProcessor.processProject(sid, vid, project)
                    else:
                        for course in data:
                            corsProcessor.processCourse(sid, vid, course)

                cursor.close()
                print("OK")
            except mysql.connector.Error as err:
                print("Error:", err)
            else:
                db.close()

            try:
                sleep(300)
            except KeyboardInterrupt:
                print("Exiting")
                break


if __name__ == '__main__':
    updateService = UpdateService()
    updateService.run()