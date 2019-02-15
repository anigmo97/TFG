#!/bin/bash
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927
echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list
sudo apt update
sudo apt-get install -y mongodb-org

#MUY IMPORTANTE 
# en ubuntu 16.04 sin esta linea fallara
sudo systemctl enable mongod.service

sudo systemctl start mongodb
sudo systemctl enable mongodb