Install on Debian Squeeze (minimal required steps)
=============
sudo apt-get install ntp
# download jdk from oracle http://www.oracle.com/technetwork/java/javase/downloads/jdk7-downloads-1880260.html
sudo mkdir /usr/lib64/jvm
sudo tar zxvf jdk-7u17-linux-x64.tar.gz -C /usr/lib64/jvm/
sudo update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk1.7.0_17/bin/java 1066
sudo update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/jdk1.7.0_17/bin/javac 1066
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