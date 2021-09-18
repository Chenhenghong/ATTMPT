from datetime import datetime
from json import loads
import requests
import urllib.parse
import pandas as pd

def convert_unix_time_in_mills_to_date(unix_time_ms):
    time_stamp_sec = int(unix_time_ms) / 1000
    return datetime.fromtimestamp(time_stamp_sec).strftime('%Y-%m-%d %H:%M:%S')

def convert_date_to_unix_time_in_mills(date):
    timestamp = datetime.strptime(date, '%Y-%m-%d %H:%M:%S').timestamp()
    return int(timestamp)


# sample reuqest for 1m Kline : https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000&startTime=1630707600000&endTime=1630764393000

def send_request(start_time_mill, end_time_mill):
    api_domain = "https://api.binance.com/api"

    request_args = {'symbol': 'BTCUSDT', 'interval': '1m', 'limit': '1000', 'startTime': start_time_mill, 'endTime': end_time_mill}
    request_url = api_domain + "/v3/klines?" + urllib.parse.urlencode(request_args)
    response = requests.get(request_url)
    response_json = response.json()
    return response_json
  #  df = pd.DataFrame([])
  #  df.append(response_json)
    df = pd.DataFrame(response_json)
    # 开盘时间 开盘价 最高价 最低价 收盘价 成交量 收盘时间 成交额 成交笔数 主动买入成交量 主动买入成交额
    # print(response_json)
    df.to_csv('sample.csv', index=False, sep="\t")



def main():
    baseline_start_sec = int(datetime(2021, 8, 1, 0, 0, 0).timestamp())
    baseline_end_sec = int(datetime(2021, 8, 2, 0, 0, 0).timestamp())
    df = pd.DataFrame([])

    dt_index_sec = baseline_start_sec
    # each request store data within 8 hour (480 mins)
    time_offset = 28800

    while (dt_index_sec < baseline_end_sec):
        start_time_mill = dt_index_sec * 1000
        end_time_mill = (dt_index_sec + time_offset - 60) * 1000
        response_data = send_request(start_time_mill, end_time_mill)
        df = df.append(response_data, ignore_index = True)
        dt_index_sec = dt_index_sec + time_offset

    df.to_csv('sample.csv', index=False, sep="\t", header=["开盘时间", "开盘价", "最高价", "最低价", "收盘价", "成交量", "收盘时间", "成交额", "成交笔数", "主动买入成交量", "主动买入成交额", "可忽略"])



   
#    df1 = pd.DataFrame([['1', '2', '3'], ['4', '5', '6']])
#    df1 = df1.append([['4', '5', '6'], ['7', '8', '9']], ignore_index=True)
#    df1 = df1.append([['7', '8', '9'], ['10', '11', '12']], ignore_index=True)
#    df1.to_csv('ans.csv', index=False, sep="\t", header=["开盘时间", "开盘价", "最高价"])

if __name__ == '__main__':
    main()