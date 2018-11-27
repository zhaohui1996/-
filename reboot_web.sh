#demo.sh                                                                                                       

#!/bin/bash
echo "---------------------"
echo "杀死nginx和uwsgi"
pkill -9 nginx
echo "killed nginx"
pkill -9 uwsgi
echo "killed uwsgi"
echo "----------------------"
echo "重启nginx和uwsgi"
nginx
uwsgi /root/web/flask/uwsgi_conf.ini
echo "----------------------"
