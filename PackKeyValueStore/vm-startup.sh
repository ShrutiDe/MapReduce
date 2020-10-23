sudo su -
apt-get -y update
apt-get -y install git
apt -y install default-jre
apt -y install default-jdk
git clone https://github.com/ShrutiDe/MapReduce.git
cd MapReduce/PackKeyValueStore/src/
javac -cp *.jar KeyValueStore.java
java -cp *.jar KeyValueStore 

EOF
