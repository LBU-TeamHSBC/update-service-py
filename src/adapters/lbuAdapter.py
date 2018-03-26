from adapters import Adapter
from config import config
import requests

class LbuAdapter(Adapter):
    BASE_URL = config['lbu']['base_url']

    def getData(self):
        data = []
        r = requests.get(LbuAdapter.BASE_URL + '/course/' + str(self.user_id))
        courses = r.json()
        
        for course in courses:
            course['id'] = course['course_id']
            del(course['course_id'])
            data.append(course)
        
        return data