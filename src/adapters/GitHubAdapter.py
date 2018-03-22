from adapters import Adapter
from config import config
import requests

class GitAdapter(Adapter):
    BASE_URL = config['git']['base_url']

    def getData(self):
        r = requests.get(GitAdapter.BASE_URL + '/repos/' + str(self.user_id))
        data = []
        for repo in r.json():
            langs = self.getLanguageTags(repo['name'])
            repo_stats = {
                'id': repo['id'],
                'name': repo['name'],
                'rating': repo['stars'],
                'created': repo['created_at'].split('T')[0],
                'updated': repo['updated_at'].split('T')[0],
                'lines_of_code': sum(langs.values()),
                'tags': {} }
            for lang in langs:
                repo_stats['tags'][lang] = int(langs[lang] / repo_stats['lines_of_code'] * 100)
            data.append(repo_stats)
        return data

    def getLanguageTags(self, repo_name):
        url = '{}/repos/{}/{}/languages'.format(GitAdapter.BASE_URL, str(self.user_id), repo_name)
        r = requests.get(url)
        return r.json()