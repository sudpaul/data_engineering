
import sys
import os
import requests
import json
from requests.auth import HTTPBasicAuth
from argparse import ArgumentParser
from pathlib import Path

parser = ArgumentParser(description="Captures parameters to process ASL file")
parser.add_argument("username", help="User to authenticate as")
parser.add_argument("password", help="Password for the user")
parser.add_argument("url", help="url of the api")
parser.add_argument("datapath", help="The full path for the stage file")
parser.add_argument("object_name", help="ASL file name")
parser.add_argument("http_proxy", help="proxy server")

args = parser.parse_args()


def extract_asl_data(path = "path", url = 'url', usr = "user", pwd = "password",http_proxy  = "proxy",filename = "file.json"):
    """
    This Function calls ASL API to extract Australian School List from ACARA
    Build Response header parameters and call API to get All Schools Data from API
    """

    https_proxy = http_proxy.replace('http','https')

    proxyDict = { 
                  "http"  : http_proxy, 
                  "https" : https_proxy
                }

    headers = { 'Accept':'text/json'}
    response = requests.get(url, auth=HTTPBasicAuth(usr,  pwd), headers=headers, proxies = proxyDict)
    try:
        response.raise_for_status()
    except:
        raise Exception("API returned error")
        return
    try:
        response = response.json()
        destination = path+filename
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(destination, 'w') as file:
            json.dump(response, file)
    except:
        raise Exception("An error occurred while processing file")
        return

#Execute the code
if __name__ == '__main__':
    extract_asl_data(usr = args.username, pwd = args.password, url = args.url,path = args.datapath, filename = args.object_name, http_proxy = args.http_proxy)
