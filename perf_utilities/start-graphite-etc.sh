#!/bin/sh
sudo service apache2 reload
sudo service apache2 restart
sudo service carbon-cache restart
cd elasticsearch-0.90.9/
bin/elasticsearch -f
