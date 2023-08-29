#Class creation to create connector card
import urllib3
import json
from socket import timeout

# webhook = Microsoft web hook url (Team channel url for message posting)
# post_message = 'Message to post on team channel'
class TeamsWebhookException(Exception):
    """custom exception for failed webhook call"""
    pass


class ConnectorCard:
    def __init__(self, hookurl, http_timeout=60):
        self.http = urllib3.PoolManager()
        self.payload = {}
        self.hookurl = hookurl
        self.http_timeout = http_timeout

    def text(self, mtext):
        self.payload["text"] = mtext
        return self

    def send(self):
        headers = {"Content-Type":"application/json"}
        r = self.http.request(
                'POST',
                f'{self.hookurl}',
                body=json.dumps(self.payload).encode('utf-8'),
                headers=headers, timeout=self.http_timeout)
        if r.status == 200: 
            return True
        else:
            
            raise TeamsWebhookException(r.reason)


if __name__ == "__main__":
    myTeamsMessage = ConnectorCard(webhook)
    myTeamsMessage.text(post_message)
    myTeamsMessage.send()