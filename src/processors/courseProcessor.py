class CourseProcessor(object):
    def __init__(self, db):
        self.db = db
    
    def processCourse(self, sid, vid, course):
        course_db_id = self._insertCourse(vid, course)

        cursor = self.db.cursor(buffered=True)
        cursor.execute('''DELETE FROM course_tag WHERE course_id=%s''', (course_db_id, ))
        for tag in course['tags']:
            tag_id = self._insertTag(tag)
            cursor.execute('''INSERT INTO
                course_tag (course_id, tag_id, weighting)
                VALUES (%s, %s, %s)''', (
                    course_db_id,
                    tag_id,
                    course['tags'][tag]))

        cursor.execute('''REPLACE INTO
            student_course (student_id, course_id, progress)
            VALUES (%s, %s, %s)''', (
                sid,
                course_db_id,
                course['progress']))
        
        for course_module in course['modules']:
            cmodule_id = self._insertCourseModule(course_db_id, course_module)
            self._insertStudentCourseModule(sid, cmodule_id, course_module['progress'])
        cursor.close()
    
    def _dbIdOrNone(self, table, where):
        cursor = self.db.cursor(buffered=True)
        keys = list(where.keys())
        where_statement = ' AND '.join(['{}=%s'.format(k) for k in keys])
        where_vals = [where[k] for k in keys]
        sql = '''SELECT id FROM {} WHERE {}'''.format(table, where_statement)
        cursor.execute(sql, where_vals)
        db_id = None
        if cursor.rowcount > 0:
            db_id = cursor.fetchone()[0]
        cursor.close()
        return db_id
    
    def _insertCourse(self, vid, course):
        course_db_id = self._dbIdOrNone('course', {'vendor_id': vid, 'course_id': course['id']})
        if course_db_id is None:
            cursor = self.db.cursor(buffered=True)
            cursor.execute('''INSERT INTO
                course (course_id, vendor_id, name, rating, participant_count)
                VALUES (%s, %s, %s, %s, %s)''', (
                    course['id'],
                    vid,
                    course['name'],
                    course['rating'],
                    course['participant_count']))
            course_db_id = cursor.lastrowid
            cursor.close()
        return course_db_id
    
    def _insertCourseModule(self, cid, module):
        cmodule_db_id = self._dbIdOrNone('course_module', {'course_id': cid, 'module_id': module['id']})
        if cmodule_db_id is None:
            cursor = self.db.cursor(buffered=True)
            cursor.execute('''INSERT INTO
                course_module (module_id, course_id, name, weighting)
                VALUES (%s, %s, %s, %s)''', (
                    module['id'],
                    cid,
                    module['name'],
                    module['weighting']))
            cmodule_db_id = cursor.lastrowid
            cursor.close()
        return cmodule_db_id

    def _insertStudentCourseModule(self, sid, cid, progress):
        cursor = self.db.cursor(buffered=True)
        cursor.execute('''REPLACE INTO
            student_course_module (student_id, course_module_id, progress)
            VALUES (%s, %s, %s)''', (
                sid,
                cid,
                progress))
        cursor.close()

    def _insertTag(self, tag):
        tagId = self._dbIdOrNone('tag', {'name': tag})
        if tagId is None:
            inCursor = self.db.cursor()
            inCursor.execute('''INSERT INTO tag (name) VALUES (%s)''', (tag,))
            tagId = inCursor.lastrowid
            inCursor.close()
        return tagId
