#!/bin/bash
cd $(dirname $0)
if ps aux | grep -v 'grep' | grep python | grep -q $1 
then
	pid=$(ps aux | grep -v 'grep' |grep 'python'|grep $1| awk '{print $2}');
	kill -9 ${pid};
fi

nohup python -u $1 &> out &
