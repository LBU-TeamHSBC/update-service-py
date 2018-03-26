class CourseProcessor(object):
    def __init__(self, db):
        self.db = db
    
    def processCourse(self, sid, vid, course):
        # Check if course exists in DB, create if not
        cursor = self.db.cursor(buffered=True)

        cursor.execute('''SELECT id
            FROM course
            WHERE vendor_id=%s AND course_id=%s''', (
                vid,
                course['id']))
        
        course_db_id = None
        if cursor.rowcount > 0:
            course_db_id = cursor.fetchone()[0]

        if course_db_id is None:
            cursor.execute('''INSERT INTO
                course (course_id, vendor_id, name, rating, participant_count)
                VALUES (%s, %s, %s, %s, %s)''', (
                    course['id'],
                    vid,
                    course['name'],
                    course['rating'],
                    course['participant_count']))
            course_db_id = cursor.lastrowid

        # Delete / Insert tags for course
        cursor.execute('''DELETE FROM course_tag WHERE course_id=%s''', (course_db_id, ))
        for tag in course['tags']:
            tag_id = self._insertTag(tag)
            cursor.execute('''INSERT INTO
                course_tag (course_id, tag_id, weighting)
                VALUES (%s, %s, %s)''', (
                    course_db_id,
                    tag_id,
                    course['tags'][tag]))

        # Insert entry into student_course
        cursor.execute('''REPLACE INTO
            student_course (student_id, course_id, progress)
            VALUES (%s, %s, %s)''', (
                sid,
                course_db_id,
                course['progress']))
        
        # Insert modules
        # TODO

        cursor.close()

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

# [
# 	{
#         modules: [
#             {
#                 id: course_module.id,
#                 module_id: course_module.id,
#                 name: course_module.name,
#                 weighting: course_module.weighting,
#                 progress: student_course_module.progress,
#             }
#         ]
#     }
# ]
