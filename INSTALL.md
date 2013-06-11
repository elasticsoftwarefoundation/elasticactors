Install on Debian Squeeze (minimal required steps)
=============
sudo apt-get install ntp
echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu precise main" | tee -a /etc/apt/sources.list
echo "deb-src http://ppa.launchpad.net/webupd8team/java/ubuntu precise main" | tee -a /etc/apt/sources.list
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
apt-get update
apt-get install oracle-java7-installer
# cassandra install
sudo apt-get install libjna-java
# add the following 2 lines to /etc/apt/sources.list.d/cassandra.sources.list
deb http://www.apache.org/dist/cassandra/debian 12x main
deb-src http://www.apache.org/dist/cassandra/debian 12x main
# install gpg keys
gpg --keyserver pgp.mit.edu --recv-keys F758CE318D77295D
gpg --export --armor F758CE318D77295D | sudo apt-key add -
gpg --keyserver pgp.mit.edu --recv-keys 2B5C1B00
gpg --export --armor 2B5C1B00 | sudo apt-key add -
# update apt
sudo apt-get update
sudo apt-get install cassandra