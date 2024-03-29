#!/usr/bin/python3
import requests
import json
import datetime
import hashlib
import smtplib
from email.mime.text import MIMEText

# URL for Sisense Application
baseURL = 'https://example.sisense.com/'


# Must contain ds.baseline and ds.log files
monitoringFilepath = '/home/ec2-user/'

# Sisense UI Service Account Credentials
data = {
    'username': 'serviceAccount',
    'password': 'serviceAccountPassword'
}

#For mail server integration, comment out lines 122-125 (smtplib.SMTP) section if you don't want email functionality
sender_email = 'Alert@Sender.com'
receiver_email = 'admin@example.com'
smtp_server = 'smtp.mailserver.com'
smtp_port = 587
smtp_username = 'Alert@Sender.com'
smtp_password = 'SMTPPassword'

now = datetime.datetime.now()
ts = now.strftime("%Y-%m-%d_%H:%M")

authURL = 'api/v1/authentication/login'

loginHeaders = {
    'accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded'
}


# Send POST request to authenticate and get access token
loginResponse = requests.post(baseURL + authURL, headers=loginHeaders, data=data)
if loginResponse.status_code == 200:
    json_data = loginResponse.json()
    if json_data.get('success'):
        access_token = json_data.get('access_token')
        print('Access Token:', access_token)
    else:
        print('Login failed:', json_data.get('message'))
        exit()
else:
    print('Failed to authenticate. Status code:', loginResponse.status_code)
    exit()

credential = 'Bearer ' + access_token


url = baseURL + 'api/v1/elasticubes/getElasticubes'  # URL to fetch Elasticubes from
headers = {
    'accept': 'application/json',
    'authorization': credential
}  # Headers with authorization token


# Read in the contents of the baseline file into a set
baseline_data = set()
with open( monitoringFilepath + 'ds.baseline', 'r+') as origBaseline:
    for line in origBaseline:
        baseline_data.add(line.strip())

# Send GET request with headers
response = requests.get(url, headers=headers)


# If the baseline file is empty, generate a baseline and add all hash_values to the file
if not baseline_data:
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
    # If successful, convert the response to json and get 'title' objects from elasticubeList
        elasticubeList = response.json()
        with open(monitoringFilepath + 'ds.baseline', 'a') as baseline:
            for elasticube in elasticubeList:
                title = elasticube.get('title')
                dataSecurityURL = baseURL + 'api/elasticubes/LocalHost/{}/datasecurity'.format(title)
                dataSecurity = requests.get(dataSecurityURL, headers=headers)
                if dataSecurity.status_code == 200:
                    prettySecurity = dataSecurity.json()
                    hash_value = hashlib.sha256((elasticube.get('title') + json.dumps(prettySecurity)).encode('utf-8')).hexdigest()
                    baseline_data.add(hash_value)
                    baseline.write(hash_value + '\n')
                else:
                    print('Failed to fetch data. Status code:', dataSecurity.status_code)
            with open( monitoringFilepath + 'ds.log', 'a') as log:
                log.write(ts + " Baseline Generated")

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # If successful, convert the response to json and get 'title' objects from elasticubeList
    elasticubeList = response.json()
    for elasticube in elasticubeList:
        title = elasticube.get('title')
        dataSecurityURL = baseURL + 'api/elasticubes/LocalHost/{}/datasecurity'.format(title)
        # Using the 'title' objects in place of the 'server' variable, perform a get request to the dataSecurityURL
        dataSecurity = requests.get(dataSecurityURL, headers=headers)
        # Check if the request was successful (status code 200)
        if dataSecurity.status_code == 200:
            prettySecurity = dataSecurity.json()
            # Print each of the returned dataSecurity objects for each server
            with open( monitoringFilepath + 'ds.log', 'a') as log:
                log.write('\n' + ts + ' Elasticube Name: ' + elasticube.get('title') + ' ' + (json.dumps(prettySecurity)))  # Record the timestamp, cube name, and DS rules
                hash_value = hashlib.sha256((elasticube.get('title') + json.dumps(prettySecurity)).encode('utf-8')).hexdigest()
                print(baseline_data)
                if hash_value in baseline_data:
                    log.write('\n' + ts + ' ' + elasticube.get('title') + ' MATCHES BASELINE')
                else:
                    log.write('\n' + ts + ' ' + elasticube.get('title') + ' DOES NOT MATCH BASELINE')
                    with open( monitoringFilepath + 'ds.baseline', 'a') as baseline:
                        baseline.write('\n' + hash_value)  # Record the cube name and DS rules in baseline
                    message = MIMEText('Elasticube name: ' + elasticube.get('title') + '\n\nNon-conforming Data Security Rule!')
                    message['Subject'] = 'Non-Conforming Data Security Rule'
                    message['From'] = sender_email
                    message['To'] = receiver_email
                    with smtplib.SMTP(smtp_server, smtp_port) as server:
                        server.starttls()
                        server.login(smtp_username, smtp_password)
                        server.sendmail(sender_email, receiver_email, message.as_string())
        else:
            print('Failed to fetch data. Status code:', dataSecurity.status_code)
else:
    print('Failed to fetch data. Status code:', response.status_code)
