from flask import Flask, request, jsonify
from flask import render_template
from sqlalchemy import create_engine
import fxcmpy
import pandas as pd
import json
from datetime import timedelta

from functions import get_data_all, get_st
from trade_functions import trade_main


app = Flask(__name__)

# =============================== 开平仓交易 ===============================
con = fxcmpy.fxcmpy(config_file='fxcm.cfg')
# 开空单
@app.route('/sell/form', methods=['GET', 'POST'], endpoint='sell')
def test():
    postData = request.form
    data = postData.to_dict()    # 获取到的前端数据结构转为 dict
    data['msg'] = 'create_sell'
    info = trade_main(data)
    return info

# 开多单
@app.route('/buy/form', methods=['GET', 'POST'], endpoint='buy')
def test():
    postData = request.form
    data = postData.to_dict()
    data['msg'] = 'create_sell'
    main(data)
    info = trade_main(data)
    return info

# =============================== 前端路由维护 ===============================
# 黄金_美元 首页
@app.route('/', methods=['GET', 'POST'], endpoint='index')
def index():
    return render_template("view_xauusd.html")

# 黄金_美元 页面
@app.route('/XAU_USD', methods=['GET', 'POST'], endpoint='xau_usd')
def html_xauusd():
    return render_template("view_xauusd.html")

# 欧元_美元 页面
@app.route('/EUR_USD', methods=['GET', 'POST'], endpoint='eur_usd')
def html_eurusd():
    return render_template("view_eurusd.html")

# 英镑_美元 页面
@app.route('/GBP_USD', methods=['GET', 'POST'], endpoint='gbp_usd')
def html_gbpusd():
    return render_template("view_gbpusd.html")

# 美元_日元 页面
@app.route('/USD_JPY', methods=['GET', 'POST'], endpoint='usd_jpy')
def html_usdjpy():
    return render_template("view_usdjpy.html")


# =============================== k线/指标/信号 API数据地址 ===============================
# ===== 黄金_美元
# 组合数据 1分钟 
@app.route('/api/v1/XAU_USD/ALL/M1')
def data_xau_m1():
    return get_data_all('M1', 'XAU_USD')

# 组合数据 5分钟
@app.route('/api/v1/XAU_USD/ALL/M5')
def data_xau_m5():
    return get_data_all('M5', 'XAU_USD')

# 组合数据 30分钟
@app.route('/api/v1/XAU_USD/ALL/M30')
def data_xau_m30():
    return get_data_all('M30', 'XAU_USD')

# 组合数据 4小时
@app.route('/api/v1/XAU_USD/ALL/H4')
def data_xau_h4():
    return get_data_all('H4', 'XAU_USD')

@app.route('/api/v1/XAU_USD/ALL/ST')
def data_xau_st():
    return get_st('XAU_USD')


# ===== 欧元_美元
# 组合数据 1分钟 
@app.route('/api/v1/EUR_USD/ALL/M1')
def data_eur_m1():
    return get_data_all('M1', 'EUR_USD')

# 组合数据 5分钟
@app.route('/api/v1/EUR_USD/ALL/M5')
def data_eur_m5():
    return get_data_all('M5', 'EUR_USD')

# 组合数据 30分钟
@app.route('/api/v1/EUR_USD/ALL/M30')
def data_eur_m30():
    return get_data_all('M30', 'EUR_USD')

# 组合数据 4小时
@app.route('/api/v1/EUR_USD/ALL/H4')
def data_eur_h4():
    return get_data_all('H4', 'EUR_USD')


@app.route('/api/v1/EUR_USD/ALL/ST')
def data_eur_st():
    return get_st('EUR_USD')


# ===== 英镑_美元
# 组合数据 1分钟 
@app.route('/api/v1/GBP_USD/ALL/M1')
def data_gbp_m1():
    return get_data_all('M1', 'GBP_USD')

# 组合数据 5分钟
@app.route('/api/v1/GBP_USD/ALL/M5')
def data_gbp_m5():
    return get_data_all('M5', 'GBP_USD')

# 组合数据 30分钟
@app.route('/api/v1/GBP_USD/ALL/M30')
def data_gbp_m30():
    return get_data_all('M30', 'GBP_USD')

# 组合数据 4小时
@app.route('/api/v1/GBP_USD/ALL/H4')
def data_gbp_h4():
    return get_data_all('H4', 'GBP_USD')


@app.route('/api/v1/GBP_USD/ALL/ST')
def data_gbp_st():
    return get_st('GBP_USD')


# ===== 美元_日元
# 组合数据 1分钟 
@app.route('/api/v1/USD_JPY/ALL/M1')
def data_usd_m1():
    return get_data_all('M1', 'USD_JPY')

# 组合数据 5分钟
@app.route('/api/v1/USD_JPY/ALL/M5')
def data_usd_m5():
    return get_data_all('M5', 'USD_JPY')

# 组合数据 30分钟
@app.route('/api/v1/USD_JPY/ALL/M30')
def data_usd_m30():
    return get_data_all('M30', 'USD_JPY')

# 组合数据 4小时
@app.route('/api/v1/USD_JPY/ALL/H4')
def data_usd_h4():
    return get_data_all('H4', 'USD_JPY')


@app.route('/api/v1/USD_JPY/ALL/ST')
def data_usd_st():
    return get_st('USD_JPY')


if __name__ == "__main__":
    app.run(debug=True)
