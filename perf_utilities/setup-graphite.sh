#!/bin/bash
# ------------------------------------------------------------------
# [amatevos] Setup Graphite, setup Grafana, download Elasticsearch
#          Caveats:
#              (1) using * in CORS, can change that to use a specific FQDN.
#              Currently mesoscontroller2 or mesoscontroller2.sl.cloud9.ibm.com can access graphite, but * is usually unsafe.
#              (2) You must change apache2 port to be 8080
#

FILE="/tmp/out.$$"
GREP="/bin/grep"
#....
# Make sure only root can run our script
if [ "$(id -u)" != "0" ]; then
    echo "This script must be run as root!" 1>&2
    exit 1
fi

echo '
Important! This will change your apache2 port to be listening on port 8080 (not 80, which is default)

Give fully qualified domain name (FQDN) of graphite/grafana server.
Leave out http(s):// (eg, mesoscontroller2.ibm.com):'
read graphite_url

#grafana_url=$graphite_url

#fqn=$(host -TtA $(hostname -s)|grep "has address"|awk '{print $1}') ; \
#if [[ "${fqn}" == "" ]] ; then fqn=$(hostname -s) ; fi ; \
#graphite_url="${fqn}"


apt-get update && apt-get dist-upgrade -y
apt-get install graphite-carbon graphite-web apache2 libapache2-mod-wsgi curl -y
echo 'CARBON_CACHE_ENABLED=true' >> /etc/default/graphite-carbon
service carbon-cache restart
graphite-manage syncdb
chown _graphite:_graphite /var/lib/graphite/graphite.db
#INSTEAD of Header set Access-Control-Allow-Origin "http://$grafana_url", we will just say *, its unsafe!
tee /etc/apache2/sites-available/graphite-web.conf <<DELIM
<VirtualHost *:8080>
	Header set Access-Control-Allow-Origin "*"
	Header set Access-Control-Allow-Methods "GET, OPTIONS"
	Header set Access-Control-Allow-Headers "origin, authorization, accept"
	ServerName $graphite_url

	Alias /grafana /usr/share/graphite-web/grafana
        <Location "/grafana">
                SetHandler None
        </Location>
DELIM
grep -E "^\s" /usr/share/graphite-web/apache2-graphite.conf >> /etc/apache2/sites-available/graphite-web.conf
echo '</VirtualHost>' >> /etc/apache2/sites-available/graphite-web.conf
a2ensite graphite-web
a2dissite 000-default #remove default site
a2enmod headers
service apache2 reload
service apache2 restart

mkdir -p /usr/share/graphite-web/grafana/
wget http://grafanarel.s3.amazonaws.com/grafana-1.8.1.tar.gz
wget https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-0.90.9.zip
tar xvzf grafana-1.8.1.tar.gz  --strip-components=1 -C /usr/share/graphite-web/grafana
cp /usr/share/graphite-web/grafana/config.sample.js /usr/share/graphite-web/grafana/config.js
cd /usr/share/graphite-web/grafana
sed -i s/my.graphite.server.com/$graphite_url/g config.js
sed -i s/my.elastic.server.com/$graphite_url/g config.js

echo '
!!Important!! You *MUST* do the following 2 steps to finish setup

1) Edit  /etc/apache2/ports.conf, change port 80 to be 8080

2) To point to Graphite & Elasticsearch urls, edit Grafana config,
    /usr/share/graphite-web/grafana/config.js
Look for
    line:39 // Graphite & Elasticsearch example setup
and (line 40, line 53) remove the enclosing "/*" and "*/" for that block.

+++++++++++++++++++++++++++++++++++
How to Start
+++++++++++++++++++++++++++++++++++
Grafana requires Elasticsearch
It is downloaded to current folder . Unzip and run by
    ./bin/elasticsearch
or
    ./bin/elasticsearch -f #to run it in foreground and see stdout elasticsearch log statements


Important! Port 8080 and 9200 must be available from wherever you are browsing to graphite'
echo "
	* http://$graphite_url:8080 - graphite
	* http://$graphite_url:8080/grafana - Grafana
	* http://$graphite_url:9200 - elasticsearch (used to save && search grafana dashboard)
"
