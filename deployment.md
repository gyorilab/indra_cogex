Deploying on an AWS EC2 server
==============================

Instance setup
--------------
On an EC2 server running Ubuntu 20.04, follow the instructions at
https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-neo4j-on-ubuntu-20-04
and do

```
sudo apt-get update
sudo apt-get install apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://debian.neo4j.com/neotechnology.gpg.key | sudo apt-key add -
sudo add-apt-repository "deb https://debian.neo4j.com stable 4.4"
sudo apt-get install neo4j
```

In the config file, update the following lines:
Set the default database to indra:
```
dbms.default_database=indra
```
EC2 instances have small default root volumes so it makes sense to move
neo4j's data folder to some other mounted volume e.g.,
```
dbms.directories.data=/data/neo4j/data
```
To accept non-local connections, set
```
dbms.default_listen_address=0.0.0.0
```

Now run `import.sh` to fill up the database with content and then start
the service with
```
sudo neo4j start
```

Once the service has started and accepts connections, indexes can be built 
by running `build_extra_indexes.sh`.

In terms of authentication, one simple way to set up a custom password
initially is to connect to the neo4j browser and do it via the web UI.

Networking setup
----------------
To communicate with neo4j directly from outside, the 7687 port (by default)
needs to be exposed for bolt protocol connections. To avoid exposing the
instance to the outside world directly, it makes sense to set up a load
balancer. Only network load balancers support bolt so that has to be used,
ideally with an IP-based target group that points to the private IP of the
given EC2 instance. The instance's security group can then just allow
incoming connections on port 7687 to the private IP (range) of the load
balancer.
