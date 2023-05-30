import pandas as pd
import requests
import json
from requests import Response
import ast
import os
import boto3
from botocore.exceptions import ClientError
import logging
import matplotlib.pyplot as plt
import io
euro = []
data = []
daterange = pd.date_range("2021, 1, 1", "2021, 12, 1")
for single_date in daterange:
    response: Response = requests.get(
        single_date.strftime('https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?date=%Y%m%d&json'))
    data.append(response.json())

with open('data.json', 'w') as outfile:
    json.dump(data, outfile)

df = pd.read_json(r'data.json')
df.to_csv(r'data.csv', index=None)
data = []
data = pd.read_csv('data.csv', delimiter=',')
print('OK')


for i in range(data.shape[0]):
    for j in range(data.shape[1] - 1):
        if "Євро" in data.loc[i][str(j)]:
            euro.append(ast.literal_eval(data.loc[i][str(j)]))

dollar = []
for i in range(data.shape[0]):
    for j in range(data.shape[1] - 1):
        if "Долар" in data.loc[i][str(j)]:
            dollar.append(ast.literal_eval(data.loc[i][str(j)]))

df = pd.DataFrame(euro)
df.to_csv(r'euro.csv', index=None)
df = pd.DataFrame(dollar)
df.to_csv(r'dollar.csv', index=None)

def upload_(file_name, bucket, object_name=None):#завантаження на бакет

    if object_name is None:
        object_name = os.path.basename(file_name)

    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

upload_('data.csv', 'clouds-lab4')
upload_('euro.csv', 'clouds-lab4')
upload_('dollar.csv', 'clouds-lab4')


s3 = boto3.resource('s3')
# choose which file to read
obj = s3.Object('clouds-lab4', "euro.csv")
obj1 = s3.Object('clouds-lab4', "dollar.csv")
# get only the body of the object
body = obj.get()['Body'].read()
body1 = obj1.get()['Body'].read()
body_csv = body.decode("utf-8")
body1_csv = body1.decode("utf-8")
data = io.StringIO(body_csv)
data1 = io.StringIO(body1_csv)
dfnew = pd.read_csv(data, sep=",")
df = pd.read_csv(data1, sep=",")

# set size
plt.figure(figsize=(20,8))
 
# naming the x and y axis
plt.ylabel('Currency')
plt.xlabel('Date')
 
# add grid
plt.grid(axis='y', color='0.95')
 
x = dfnew["exchangedate"]
y1 = dfnew["rate"]
y2 = df["rate"]
x1 = df["exchangedate"]
 
# manage x axis (355 records kinda too much)
plt.xticks(range(0,len(x))[::7], rotation='vertical')  
 
plt.plot(x, y1, label = "EUR")
plt.plot(x1, y2, label = "USD")
 
# creating legend
plt.legend()


# save to a file
plt.savefig('plot.png')
s3_client = boto3.client('s3')
s3_client.upload_file('plot.png', 'clouds-lab4', 'plot.png')
