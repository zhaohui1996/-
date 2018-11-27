from sqlalchemy import create_engine
import pymysql


# 源数据库
# conn = create_engine('mysql+pymysql://zen:root@162.211.225.138:3306/zen?charset=utf8')

# web本地数据库
# conn = create_engine('mysql+pymysql://root:12345@localhost/Zen_test?charset=utf8', echo=False)

# 本地调用远程数据库
conn = create_engine('mysql+pymysql://root:12345@47.98.175.31:3306/Zen_test?charset=utf8', echo=False)

