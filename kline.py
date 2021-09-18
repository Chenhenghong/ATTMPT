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
    # 开盘时间 开盘价 最高价 最低价 收盘价 成交量 收盘时间 成交额 成交笔数 主动买入成交量 主动买入成交额 可忽略数字
    response_json = response.json()
    return response_json

def main():
    baseline_start_sec = int(datetime(2021, 8, 1, 0, 0, 0).timestamp())
    baseline_end_sec = int(datetime(2021, 9, 1, 0, 0, 0).timestamp())
    df = pd.DataFrame([])

    dt_index_sec = baseline_start_sec
    # each request store data within 8 hour (480 mins)
    time_offset = 28800

    while (dt_index_sec < baseline_end_sec):
        start_time_mill = dt_index_sec * 1000
        end_time_mill = (dt_index_sec + time_offset - 60) * 1000
        if (end_time_mill > baseline_end_sec * 1000):
            end_time_mill = baseline_end_sec * 1000
        response_data = send_request(start_time_mill, end_time_mill)
        dt_index_sec = dt_index_sec + time_offset
        # there is no trade data exists when Binance is on maintenance (e.g. Aug 13 10AM to 15PM UTC+9)
        if (len(response_data) > 0):
            df = df.append(response_data, ignore_index = True)

    df.drop(df.columns[len(df.columns)-1], axis=1, inplace=True)
    df = df.set_axis(["开盘UnixTime", "开盘价", "最高价", "最低价", "收盘价", "成交量", "收盘UnixTime", "成交额", "成交笔数", "主动买入成交量", "主动买入成交额"], axis='columns')
    
    column_name = df.columns.tolist()
    column_name.insert(0, "开盘DateTime")
    column_name.insert(7, "收盘DateTime")
    df = df.reindex(columns=column_name)
    df["开盘DateTime"] = df["开盘UnixTime"].astype(int)
    df["开盘DateTime"] = df["开盘DateTime"].apply(lambda x: convert_unix_time_in_mills_to_date(x))
    df["收盘DateTime"] = df["收盘UnixTime"].astype(int)
    df["收盘DateTime"] = df["收盘UnixTime"].apply(lambda x: convert_unix_time_in_mills_to_date(x))
    df.to_csv('test.csv', index=False, sep="\t")

if __name__ == '__main__':
    main()