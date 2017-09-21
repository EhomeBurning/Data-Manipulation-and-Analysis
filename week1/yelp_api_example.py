"""Example for signing a search request using the oauth2 library."""


#pip install yelp
from yelp.client import Client
from yelp.oauth1_authenticator import Oauth1Authenticator

# Get your Yelp API keys from here:
# https://www.yelp.com/developers/v2/manage_api_keys
# Fill in these values
auth = Oauth1Authenticator(
                           consumer_key='',
                           consumer_secret='',
                           token='',
                           token_secret=''
                           )

client = Client(auth)
response = client.search('San Francisco')
businesses = response.businesses
# a few relevant fields
response.businesses[0].name
response.businesses[0].rating
#for more go to: https://github.com/Yelp/yelp-python


