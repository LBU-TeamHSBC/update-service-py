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
            self.con = mysql.connector.connect(**config['db'])
            cursor = self.con.cursor()
            cursor.execute(UpdateService.LINKED_ACCOUNTS_SQL)
            for (sid, oauth, vid, cat) in cursor:
                AdapterClass = UpdateService.SERVICE_MAP[vid]
                data = AdapterClass(sid, oauth).getData()
                if cat == UpdateService.SERVICE_TYPE_MAP['project']:
                    for project in data:
                        self.processProject(sid, vid, project)
                else:
                    print('course')
            cursor.close()
        except mysql.connector.Error as err:
            print(err)
        else:
            self.con.close()
    
    def _projectExists(self, sid, vid, pid):
        get_project_sql = '''SELECT id FROM student_project
            WHERE student_id=%s AND vendor_id=%s AND project_id=%s'''
        cursor = self.con.cursor(buffered=True)
        cursor.execute(get_project_sql, (sid, vid, pid))
        result = cursor.rowcount > 0
        cursor.close()
        return result
    
    def insertProject(self, sid, vid, project, update=False):
        insert_project_sql = '''INSERT INTO student_project (
                student_id, vendor_id, project_id, name,
                rating, lines_of_code, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

        update_project_sql = '''UPDATE student_project SET
            name=%s, rating=%s, lines_of_code=%s,
            created_at=%s, updated_at=%s'''

        data = (
            sid,
            vid,
            project['id'],
            project['name'],
            project['rating'],
            project['lines_of_code'],
            project['created'],
            project['updated'])

        cursor = self.con.cursor()
        if update == True:
            cursor.execute(update_project_sql, data[3:])
        else:
            cursor.execute(insert_project_sql, data)
        cursor.close()

    def processProject(self, sid, vid, project):
        update_project = self._projectExists(sid, vid, project['id'])
        print(sid, vid, project['id'], update_project)
        self.insertProject(sid, vid, project, update_project)


if __name__ == '__main__':
    updateService = UpdateService()
    updateService.run()