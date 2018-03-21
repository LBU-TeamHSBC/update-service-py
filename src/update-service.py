#!/usr/bin/env python3

import mysql.connector
import requests
from pprint import pprint


class GitAdapter(object):
    BASE_URL = 'http://127.0.0.1:3000/git/'

    def __init__(self, user_id):
        self.user_id = user_id

    def getRepos(self):
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


class UpdateService(object):
    LINKED_ACCOUNTS_SQL = """SELECT u.username, sv.student_id, sv.oauth_token, v.id AS vendor_id, v.category
        FROM student_vendor AS sv
        INNER JOIN student s ON s.id=sv.student_id
        INNER JOIN user u ON u.id=s.user_id
        INNER JOIN vendor v ON v.id=sv.vendor_id"""


#gitUser = GitAdapter(1)
#pprint(gitUser.getRepos())
