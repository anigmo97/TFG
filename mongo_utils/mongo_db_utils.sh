#!/bin/bash

#start mongodb
sudo systemctl start mongod
sudo service mongod start

# see status
sudo systemctl status mongod

#enable automatically starting MongoDB when the system starts
sudo systemctl enable mongod

#stop mongodb
sudo systemctl stop mongod
sudo service mongod stop

# restart
sudo service mongod restart


#mongodb command
mongo

# see databases path
grep dbPath /etc/mongod.conf

# see mongo processes and kill them
top | grep mongo 
kill -9 pid


ps -A  | grep mongo | cut -d ' ' -f1
ps -A  | grep chrome | cut -d ' ' -f1 | grep -E '..*'


# see port used by mongo
sudo lsof -i -P -n | grep LISTEN | grep mongo

netstat -plntu

db.[CollectionName].find().forEach(function(d){ db.getSiblingDB([Database])[CollectionName].insert(d); });