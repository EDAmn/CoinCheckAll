import json
import requests
import pandas as pd


# insert your API key here
API_KEY = '22FEsWegImNeAgxKXRNVS3No3O4'

# make API request
res = requests.get('https://api.glassnode.com/v1/metrics/indicators/sopr',
    params={'a': 'BTC', 'api_key': API_KEY})
res2 = requests.get("https://api.glassnode.com/v1/metrics/addresses/active_count",
    params ={"a": "BTC", "api_key": API_KEY})
res3 = requests.get("https://api.glassnode.com/v1/metrics/addresses/count",
    params ={"a": "BTC", "api_key": API_KEY})

res4 = requests.get("https://api.glassnode.com/v1/metrics/blockchain/block_height",
    params ={"a": "BTC", "api_key": API_KEY})

res5 = requests.get("https://api.glassnode.com/v1/metrics/market/marketcap_usd",
    params ={"a": "BTC", "api_key": API_KEY})


# convert to pandas dataframe
df = pd.read_json(res.text, convert_dates=['t'])
df2 = pd.read_json(res2.text, convert_dates=["t"])
df3 = pd.read_json(res3.text, convert_dates=["t"])

df4 = pd.read_json(res4.text, convert_dates=["t"])

df5 = pd.read_json(res5.text, convert_dates=["t"])

print(df)
print(df2)
print(df3)

print(df4)

print(df5)

storeCSVPath = (r"C:\Users\Administrator\Desktop\PythonScripts\BTC_Analysis\DataOut\Glass\sopr.csv")
df.to_csv(storeCSVPath, encoding='utf-8', index=False)

storeCSVPath = (r"C:\Users\Administrator\Desktop\PythonScripts\BTC_Analysis\DataOut\Glass\active_count.csv")
df2.to_csv(storeCSVPath, encoding='utf-8', index=False)

storeCSVPath = (r"C:\Users\Administrator\Desktop\PythonScripts\BTC_Analysis\DataOut\Glass\count.csv")
df3.to_csv(storeCSVPath, encoding='utf-8', index=False)

storeCSVPath = (r"C:\Users\Administrator\Desktop\PythonScripts\BTC_Analysis\DataOut\Glass\block_height.csv")
df4.to_csv(storeCSVPath, encoding='utf-8', index=False)

storeCSVPath = (r"C:\Users\Administrator\Desktop\PythonScripts\BTC_Analysis\DataOut\Glass\marketcap_usd.csv")
df5.to_csv(storeCSVPath, encoding='utf-8', index=False)