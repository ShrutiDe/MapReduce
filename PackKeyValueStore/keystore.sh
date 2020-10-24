sudo su -
cd /
apt-get -y update
apt-get -y install git
apt -y install default-jre
apt -y install default-jdk
git clone https://github.com/ShrutiDe/MapReduce.git
cd MapReduce/MasterPack/src/MapperReducer
javac -cp "/libs/*.jar" MasterServer.java
java -cp "/libs/*.jar" MasterServer

EOF