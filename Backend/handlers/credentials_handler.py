import json
from Backend.handlers.logging_handler import logger
import os

'''
Credentials handler

Checks if credentials are present before performing any code,
Credentials required are:

> postgres url
> encrypted openAPI key
> key to decrypt openAPI key
> Reddit and PostGres credentials in the Credentials.json file

if found, they are sent to the main app
'''

try:
    assert os.path.isfile('Backend/Credentials/key.key')
    assert os.path.isfile('Backend/Credentials/encrypted.key')
    assert os.path.isfile('Backend/Credentials/url.txt')
    assert os.path.isfile('Backend/Credentials/Credentials.json')
except:
    logger.fatal("Couldn't find Credentials")
    raise Exception("Couldnt find credentials in Credentials folder")


def getCredentials():
    with open('Backend/Credentials/Credentials.json') as f:
        data = json.load(f)
        redditCredentials = data['redditCredentials'][0]
        postGreCredentials = data['postGreCredentials'][0]

    with open('Backend/Credentials/url.txt', 'r') as f:
        url = f.read()
    logger.info("Acquired Credentials")
    return redditCredentials, postGreCredentials, url
