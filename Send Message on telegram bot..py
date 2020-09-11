# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 10:30:12 2020

@author: Manish Chauhan | +91-9774083186
"""

import requests

# Dictionary with details 

'''
proxies = {
 'http': 'http://10.10.10.10:8000',
 'https': 'http://10.10.10.10:8000',
}
'''
msg="Hi User, Please drop a text from telegram on @gtl_airtel_bot to confirm the delivery. Manish Chauhan"

#Chat id if the reciver.
chat_id=-429672726

# Idendificcation token for the receiver , Generated using @BOtFather
my_token = '523741186:AAF_T8r0-RzlfRwYmvH7u9SyOAkZNwWWNWc'

# API url for telegram which is used to send messages
url = f'https://api.telegram.org/bot{my_token}/sendMessage'

# Object with detials chati_id and message
data = {'chat_id': chat_id, 'text': msg}

#Send the object to api url using the POST and Get the response in form of json object
requests.post(url, data).json()

# Request method to be used when bypassing proxies
#requests.post(url, data,proxies).json()

#Refrence https://medium.com/@wk0/send-and-receive-messages-with-the-telegram-api-17de9102ab78


