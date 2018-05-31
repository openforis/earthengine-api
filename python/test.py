import json

import requests
import sys
from flask import Flask, redirect, request
from oauth2client.client import OAuth2Credentials

import ee

app = Flask(__name__)

oauth = None
accessToken = None


@app.route('/')
def index():
  if not accessToken:
    return redirect('https://accounts.google.com/o/oauth2/v2/auth?'
                    'scope=' + ee.oauth.SCOPE + ' https://www.googleapis.com/auth/drive&'
                                                'redirect_uri=http%3A%2F%2F127.0.0.1:5001%2Foauth%2Fcallback&'
                                                'response_type=code&'
                                                'client_id=' + oauth['web']['client_id'])

  credentials = OAuth2Credentials(accessToken, None, None, None, None, None, None)
  ee.InitializeThread(credentials)
  return 'Hello, GEE!'


@app.route('/oauth/callback')
def oauth_callback():
  # TODO: Check if error is present
  authorization_code = request.values['code']
  response = requests.post(
    url='https://www.googleapis.com//oauth2/v4/token',
    data={
      'code': authorization_code,
      'client_id': oauth['web']['client_id'],
      'client_secret': oauth['web']['client_secret'],
      'redirect_uri': 'http://127.0.0.1:5001/oauth/callback',
      'grant_type': 'authorization_code'
    }
  )
  content = json.loads(response.content)
  global accessToken, tokenExpiresIn, refreshToken
  accessToken = content['access_token']
  return redirect('/')


if __name__ == '__main__':
  with open(sys.argv[1]) as oauth_file:
    oauth = json.load(oauth_file)
  app.run(threaded=True, port=5001, debug=True)
