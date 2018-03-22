class ProjectProcessor(object):
    def __init__(self, db):
        self.db = db
    
    def processProject(self, sid, vid, project):
        insert_id = self._projectExists(sid, vid, project['id'])
        if insert_id == None:
            insert_id = self._insertProject(sid, vid, project)
        else:
            self._insertProject(sid, vid, project, True)
        self._insertProjectTags(sid, insert_id, project['tags'])

    def _projectExists(self, sid, vid, pid):
        get_project_sql = '''SELECT id FROM student_project
            WHERE student_id=%s AND vendor_id=%s AND project_id=%s'''
        cursor = self.db.cursor(buffered=True)
        cursor.execute(get_project_sql, (sid, vid, pid))
        result = None
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]
        cursor.close()
        return result
    
    def _insertProject(self, sid, vid, project, update=False):
        insert_project_sql = '''INSERT INTO student_project (
                student_id, vendor_id, project_id, name,
                rating, lines_of_code, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

        update_project_sql = '''UPDATE student_project SET
            name=%s, rating=%s, lines_of_code=%s,
            created_at=%s, updated_at=%s
            WHERE student_id=%s AND vendor_id=%s AND project_id=%s'''

        project_ids = (sid, vid, project['id'])
        data = (
            project['name'],
            project['rating'],
            project['lines_of_code'],
            project['created'],
            project['updated'])

        result = None
        cursor = self.db.cursor()
        if update == True:
            cursor.execute(update_project_sql, data + project_ids)
        else:
            cursor.execute(insert_project_sql, project_ids + data)
            result = cursor.lastrowid
        cursor.close()
        return result

    def _insertTag(self, tag):
        cursor = self.db.cursor(buffered=True)
        cursor.execute('''SELECT id FROM tag WHERE name=%s''', (tag,))
        if cursor.rowcount > 0:
            tagId = cursor.fetchone()[0]
        else:
            inCursor = self.db.cursor()
            inCursor.execute('''INSERT INTO tag (name) VALUES (%s)''', (tag,))
            tagId = inCursor.lastrowid
            inCursor.close()
        cursor.close()
        return tagId
    
    def _insertProjectTag(self, cur, pid, tagId, weighting):
        insert_sql = '''INSERT INTO student_project_tag (
            student_project_id, tag_id, weighting)
            VALUES (%s, %s, %s)'''
        cur.execute(insert_sql, (pid, tagId, weighting))
    
    def _insertProjectTags(self, sid, pid, tags):
        cursor = self.db.cursor()
        cursor.execute('''DELETE FROM student_project_tag WHERE student_project_id=%s''', (pid,))
        for tag in tags:
            tagId = self._insertTag(tag.lower())
            self._insertProjectTag(cursor, pid, tagId, tags[tag])
        cursor.close()