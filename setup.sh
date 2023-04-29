pip3 install -r requirements.txt --no-cache-dir

#export SPARK_HOME=~/.local/bin/pyspark

sudo apt install openjdk-8-jre-headless

echo export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre >> ~/.bashrc
echo export PATH=$JAVA_HOME/bin:$PATH >> ~/.bashrc

source ~/.bashrc