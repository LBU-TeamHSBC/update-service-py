from adapters import Adapter
from config import config
import requests

class LbuAdapter(Adapter):
    BASE_URL = config['lbu']['base_url']

    def getData(self):
        return [{},{}]