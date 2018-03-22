class CourseProcessor(object):
    def __init__(self, db):
        self.db = db
    
    def processCourse(self, sid, vid, course):
        pass

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