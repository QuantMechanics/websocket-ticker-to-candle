import requests
import json
import os
from pathlib import Path


class TelUtil():

    def __init__(self):
        self.base_path = self.get_project_root()
        self.system_cred = self.loadJson(os.path.join(
            self.base_path, "credentials", "env_credential.json"))
        self.tel_url = "https://api.telegram.org/{secret}/sendmessage".format(
            secret=self.system_cred["telegram"]["secret"])
        self.chat_id = self.system_cred["telegram"]["chat_id"]

    def get_project_root(self) -> Path:
        return Path(__file__).parent.parent

    def loadJson(self, filename=""):  # This helps in reading JSON files
        with open(filename, mode="r") as json_file:
            data = json.load(json_file)
            return data

    # @classmethod
    def sendMsg(self, msg: str):
        try:
            payload = {'chat_id': self.chat_id, 'text': msg,
                       'disable_notification': 'false'}
            response = requests.request(
                "POST", self.tel_url, json=payload)
            return (response.text.encode('utf8'))
        except Exception as identifier:
            return identifier
