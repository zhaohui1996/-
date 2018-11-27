from datetime import timedelta
import pandas as pd
import json
import pymysql
from sqlalchemy import create_engine
from config import conn


# 1.1 k线和macd
def get_k_table(level, symbol):
    # 读取数据库
    table = f'{level}_{symbol}_newk'
    sql1 = "SELECT * FROM (SELECT * FROM `" + table + "` ORDER BY `" + table + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    k_data = pd.read_sql(sql1, conn)
    k_data['date'] = (k_data['date'] + timedelta(hours=8)).astype(str)

    # 筛选处理 k线 字段位置
    k_data = k_data.reindex(columns=['date', 'open', 'close', 'low', 'high'])

    # 计算 macd 的 dif/dea/bar值
    ma1 = 12 * 1  # 5
    ma2 = 26 * 1  # 15
    mid = 9 * 1  # 6

    k_data['ma12'] = k_data.close.rolling(window=ma1).mean()
    k_data['ma26'] = k_data.close.rolling(window=ma2).mean()
    k_data['dif'] = k_data['ma12'] - k_data['ma26']
    k_data['dea'] = k_data.dif.rolling(window=mid).mean()
    k_data['bar'] = (k_data['dif'] - k_data['dea']) * 2
    k_data = k_data.fillna('-')

    macd_data = k_data
    # k_data = k_data.iloc[40:]

    # 返回 date, k线, macd
    data = {
        'date': k_data['date'].values.tolist(),
        'k_line': k_data[['open', 'close', 'low', 'high']].values.tolist(),
        'dif': macd_data['dif'].values.tolist(),
        'dea': macd_data['dea'].values.tolist(),
        'bar': macd_data['bar'].values.tolist(),
    }
    return data


# 1.2 均线和多级别中枢
def get_q_table(level, symbol):
    # ===== 读取数据库
    table1 = f'M1_{symbol}_newmcqlist'
    sql1 = "SELECT * FROM (SELECT * FROM `" + table1 + "` ORDER BY `" + table1 + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    q_data_1m = pd.read_sql(sql1, conn)

    table2 = f'M5_{symbol}_newmcqlist'
    sql2 = "SELECT * FROM (SELECT * FROM `" + table2 + "` ORDER BY `" + table2 + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    q_data_5m = pd.read_sql(sql2, conn)

    table3 = f'M30_{symbol}_newmcqlist'
    sql3 = "SELECT * FROM (SELECT * FROM `" + table3 + "` ORDER BY `" + table3 + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    q_data_30m = pd.read_sql(sql3, conn)

    table4 = f'H4_{symbol}_newmcqlist'
    sql4 = "SELECT * FROM (SELECT * FROM `" + table4 + "` ORDER BY `" + table4 + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    q_data_4h = pd.read_sql(sql4, conn)

    q_data_1m['date'] = (q_data_1m['date'] + timedelta(hours=8)).astype(str)
    q_data_5m['date'] = (q_data_5m['date'] + timedelta(hours=8)).astype(str)
    q_data_30m['date'] = (q_data_30m['date'] + timedelta(hours=8)).astype(str)
    q_data_4h['date'] = (q_data_4h['date'] + timedelta(hours=8)).astype(str)

    # ===== 构造各级别中枢 mc_data ==> 各级别中枢
    # 重命名字段
    q_data_1m.rename(columns={'mcup': 'mcup_1m', 'mcdn': 'mcdn_1m'}, inplace=True)
    q_data_5m.rename(columns={'mcup': 'mcup_5m', 'mcdn': 'mcdn_5m'}, inplace=True)
    q_data_30m.rename(columns={'mcup': 'mcup_30m', 'mcdn': 'mcdn_30m'}, inplace=True)
    q_data_4h.rename(columns={'mcup': 'mcup_4h', 'mcdn': 'mcdn_4h'}, inplace=True)

    # 筛选字段
    data_1m = q_data_1m[['date', 'mcup_1m', 'mcdn_1m']]
    data_5m = q_data_5m[['date', 'mcup_5m', 'mcdn_5m']]
    data_30m = q_data_30m[['date', 'mcup_30m', 'mcdn_30m']]
    data_4h = q_data_4h[['date', 'mcup_4h', 'mcdn_4h']]

    if level == 'M1':
        # 筛选处理 q_data ==> ma5, ma30
        data_ma = q_data_1m.fillna('-')
        # 左连接 1分级别
        df1 = pd.merge(data_1m, data_5m, on='date', how='left')
        df2 = pd.merge(df1, data_30m, on='date', how='left')
        df3 = pd.merge(df2, data_4h, on='date', how='left')
    elif level == 'M5':
        data_ma = q_data_5m.fillna('-')
        # === 左连接
        df1 = pd.merge(data_5m, data_1m, on='date', how='left')
        df2 = pd.merge(df1, data_30m, on='date', how='left')
        df3 = pd.merge(df2, data_4h, on='date', how='left')
    elif level == 'M30':
        data_ma = q_data_30m.fillna('-')
        # === 左连接
        df1 = pd.merge(data_30m, data_1m, on='date', how='left')
        df2 = pd.merge(df1, data_5m, on='date', how='left')
        df3 = pd.merge(df2, data_4h, on='date', how='left')
    elif level == 'H4':
        data_ma = q_data_4h.fillna('-')
        # === 左连接
        df1 = pd.merge(data_4h, data_1m, on='date', how='left')
        df2 = pd.merge(df1, data_5m, on='date', how='left')
        df3 = pd.merge(df2, data_30m, on='date', how='left')
    else:
        pass

    # 缺失值处理, 由于是在1分钟上, 大级别中枢的 NaN 直接按 前值 填充 (前面有值表示中枢开始出现)
    df4 = df3.fillna(method='ffill')  # 缺失值填充为 前值
    df4 = df4.fillna(0)  # 其他缺失值填充为 0
    df4['mcup_1m'].loc[df4['mcup_1m'].diff().fillna(0) != 0] = '-'  # 断开中枢
    df4['mcdn_1m'].loc[df4['mcdn_1m'].diff().fillna(0) != 0] = '-'
    df4['mcup_5m'].loc[df4['mcup_5m'].diff().fillna(0) != 0] = '-'
    df4['mcdn_5m'].loc[df4['mcdn_5m'].diff().fillna(0) != 0] = '-'
    df4['mcup_30m'].loc[df4['mcup_30m'].diff().fillna(0) != 0] = '-'
    df4['mcdn_30m'].loc[df4['mcdn_30m'].diff().fillna(0) != 0] = '-'
    df4['mcup_4h'].loc[df4['mcup_4h'].diff().fillna(0) != 0] = '-'
    df4['mcdn_4h'].loc[df4['mcdn_4h'].diff().fillna(0) != 0] = '-'
    mc_data = df4.iloc[:, :].replace(0, '-')  # 0 转变为 -

    # ===== 返回数据: 均线, 中枢
    data = {
        'ma5': data_ma['ma5'].values.tolist(),
        'ma30': data_ma['ma30'].values.tolist(),
        'mcup_1m': mc_data['mcup_1m'].values.tolist(),
        'mcdn_1m': mc_data['mcdn_1m'].values.tolist(),
        'mcup_5m': mc_data['mcup_5m'].values.tolist(),
        'mcdn_5m': mc_data['mcdn_5m'].values.tolist(),
        'mcup_30m': mc_data['mcup_30m'].values.tolist(),
        'mcdn_30m': mc_data['mcdn_30m'].values.tolist(),
        'mcup_4h': mc_data['mcup_4h'].values.tolist(),
        'mcdn_4h': mc_data['mcdn_4h'].values.tolist(),
    }
    return data


# 1.3 策略信号
def get_st_table(level, symbol):
    # 读取数据库
    table = f'{level}_{symbol}_newst'
    sql1 = "SELECT * FROM (SELECT * FROM `" + table + "` ORDER BY `" + table + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    st_data = pd.read_sql(sql1, conn)
    st_data['date'] = (st_data['date'] + timedelta(hours=8)).astype(str)

    # 筛选策略信号 st
    mcup_rise = st_data.loc[st_data.type == "c mcup rise"][["date", "ma5"]]
    mcdn_rise = st_data.loc[st_data.type == "c mcdn rise"][["date", "ma5"]]
    mcup_fall = st_data.loc[st_data.type == "c mcup fall"][["date", "ma5"]]
    mcdn_fall = st_data.loc[st_data.type == "c mcdn fall"][["date", "ma5"]]

    # 返回4个信号点
    data = {
        'mcup_rise': mcup_rise.values.tolist(),
        'mcdn_rise': mcdn_rise.values.tolist(),
        'mcup_fall': mcup_fall.values.tolist(),
        'mcdn_fall': mcdn_fall.values.tolist(),
    }
    return data


# 0. 拼接数据
def get_data_all(level, symbol):
    k_data = get_k_table(level, symbol)
    q_data = get_q_table(level, symbol)
    st_data = get_st_table(level, symbol)
    data = {
        'k': k_data,
        'q': q_data,
        'st': st_data,
    }
    return json.dumps(data)


def get_st(symbol):
    table = f'M1_{symbol}_newst'
    sql1 = "SELECT * FROM (SELECT * FROM `" + table + "` ORDER BY `" + table + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    st_data1 = pd.read_sql(sql1, conn)
    st_data1['date'] = (st_data1['date'] + timedelta(hours=8)).astype(str)
    mcup_rise_M1 = st_data1.loc[st_data1.type == "c mcup rise"][["date", "ma5"]]
    mcdn_rise_M1 = st_data1.loc[st_data1.type == "c mcdn rise"][["date", "ma5"]]
    mcup_fall_M1 = st_data1.loc[st_data1.type == "c mcup fall"][["date", "ma5"]]
    mcdn_fall_M1 = st_data1.loc[st_data1.type == "c mcdn fall"][["date", "ma5"]]

    table = f'M5_{symbol}_newst'
    sql2 = "SELECT * FROM (SELECT * FROM `" + table + "` ORDER BY `" + table + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    st_data2 = pd.read_sql(sql2, conn)
    st_data2['date'] = (st_data2['date'] + timedelta(hours=8)).astype(str)
    mcup_rise_M5 = st_data2.loc[st_data2.type == "c mcup rise"][["date", "ma5"]]
    mcdn_rise_M5 = st_data2.loc[st_data2.type == "c mcdn rise"][["date", "ma5"]]
    mcup_fall_M5 = st_data2.loc[st_data2.type == "c mcup fall"][["date", "ma5"]]
    mcdn_fall_M5 = st_data2.loc[st_data2.type == "c mcdn fall"][["date", "ma5"]]

    table = f'M30_{symbol}_newst'
    sql3 = "SELECT * FROM (SELECT * FROM `" + table + "` ORDER BY `" + table + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    st_data3 = pd.read_sql(sql3, conn)
    st_data3['date'] = (st_data3['date'] + timedelta(hours=8)).astype(str)
    mcup_rise_M30 = st_data3.loc[st_data3.type == "c mcup rise"][["date", "ma5"]]
    mcdn_rise_M30 = st_data3.loc[st_data3.type == "c mcdn rise"][["date", "ma5"]]
    mcup_fall_M30 = st_data3.loc[st_data3.type == "c mcup fall"][["date", "ma5"]]
    mcdn_fall_M30 = st_data3.loc[st_data3.type == "c mcdn fall"][["date", "ma5"]]

    table = f'H4_{symbol}_newst'
    sql4 = "SELECT * FROM (SELECT * FROM `" + table + "` ORDER BY `" + table + "`.`date`  DESC LIMIT 1000) as b ORDER BY b.date ASC"
    st_data4 = pd.read_sql(sql4, conn)
    st_data4['date'] = (st_data4['date'] + timedelta(hours=8)).astype(str)
    mcup_rise_H4 = st_data4.loc[st_data4.type == "c mcup rise"][["date", "ma5"]]
    mcdn_rise_H4 = st_data4.loc[st_data4.type == "c mcdn rise"][["date", "ma5"]]
    mcup_fall_H4 = st_data4.loc[st_data4.type == "c mcup fall"][["date", "ma5"]]
    mcdn_fall_H4 = st_data4.loc[st_data4.type == "c mcdn fall"][["date", "ma5"]]

    # 返回4个信号点
    data = {
        'mcup_rise_m1': mcup_rise_M1.values.tolist(),
        'mcdn_rise_m1': mcdn_rise_M1.values.tolist(),
        'mcup_fall_m1': mcup_fall_M1.values.tolist(),
        'mcdn_fall_m1': mcdn_fall_M1.values.tolist(),
        'mcup_rise_m5': mcup_rise_M5.values.tolist(),
        'mcdn_rise_m5': mcdn_rise_M5.values.tolist(),
        'mcup_fall_m5': mcup_fall_M5.values.tolist(),
        'mcdn_fall_m5': mcdn_fall_M5.values.tolist(),
        'mcup_rise_m30': mcup_rise_M30.values.tolist(),
        'mcdn_rise_m30': mcdn_rise_M30.values.tolist(),
        'mcup_fall_m30': mcup_fall_M30.values.tolist(),
        'mcdn_fall_m30': mcdn_fall_M30.values.tolist(),
        'mcup_rise_h4': mcup_rise_H4.values.tolist(),
        'mcdn_rise_h4': mcdn_rise_H4.values.tolist(),
        'mcup_fall_h4': mcup_fall_H4.values.tolist(),
        'mcdn_fall_h4': mcdn_fall_H4.values.tolist(),
    }
    return json.dumps(data)
# 调试用
# if __name__ == '__main__':
#     # r = get_k_table('M1', 'XAU_USD')
#     # r = get_data_all('M1', 'XAU_USD')
#     r = get_data_all('M30', 'EUR_USD')
#     print(r)
