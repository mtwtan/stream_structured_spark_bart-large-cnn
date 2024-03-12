# Kafka streaming

- This is a sample Kafka script to generate streaming objects.
- It uses AWS MSK Serverless using IAM authentication.
- It also uses Spark to read the Amazon reviews data in S3 to produce the streaming data for Kafka.
- To get the Amazon reviews data into your S3 bucket, [use or modify the code here](/download/download.py).

## Set up Kafka client environment
- Use an appropriate instance size, example: t3.medium or m5.large for larger datasets
- This example uses an Ubuntu AMI ( `ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20231207` )
- Run the following commands:

```
export BASEDIR = "/home/ubuntu"
cd $BASEDIR

# Download hadoop, scala, and spark
curl -O https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
curl -O https://downloads.lightbend.com/scala/2.12.18/scala-2.12.18.tgz
curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz

tar -xvf hadoop-3.3.6.tar.gz
tar -xvf scala-2.12.18.tgz
tar -xvf spark-3.5.0-bin-hadoop3.tgz

sudo mv hadoop-3.3.6 /opt/
sudo mv scala-2.12.18 /opt/
sudo mv spark-3.5.0-bin-hadoop3 /opt/

cd /opt/
sudo ln -s hadoop-3.3.6 hadoop
sudo ln -s scala-2.12.18 scala
sudo ln -s spark-3.5.0-bin-hadoop3 spark

cd $BASEDIR

# Install OpenJDK
sudo apt install openjdk-17
sudo apt install openjdk-17-jdk

# Download jars
mkdir /tmp/jars
cd /tmp/jars
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar
curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.667/aws-java-sdk-bundle-1.12.667.jar
curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.1/spark-sql-kafka-0-10_2.13-3.5.1.jar
curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar
curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar

sudo cp *.jar /opt/spark/jars/

```
## Download jar file for IAM Authentication with MSK
- Download this ALL jar package from GitHub. You will need a browser to properly download this file; running a curl will only get you a 302 redirect. The [download link is here](https://github.com/aws/aws-msk-iam-auth/releases/download/v2.0.3/aws-msk-iam-auth-2.0.3-all.jar).
- After downloading the file, scp the file into your EC2 instance
```
# From your computer after ownloading aws-msk-iam-auth-2.0.3-all.jar
scp -i <key pem file> aws-msk-iam-auth-2.0.3-all.jar ubuntu@< EC2 IP address>:/home/ubuntu/

# From inside your EC2 instance
cp aws-msk-iam-auth-2.0.3-all.jar /opt/spark/jars/
```
## Continue ...
```

cd $BASEDIR

# Install Kafka client
curl -O https://downloads.apache.org/kafka/3.7.0/kafka_2.12-3.7.0.tgz
tar -xvf kafka_2.12-3.7.0.tgz
sudo mv kafka_2.12-3.7.0 /opt/
sudo ln -s kafka_2.12-3.7.0 kafka

# Install Python libraries
curl -sSL https://bootstrap.pypa.io/get-pip.py -o get-pip.py

python3 get-pip.py
python3 -m pip install pyspark
python3 -m pip install faker boto3
python3 -m pip install aws-msk-iam-sasl-signer-python
# Modify .bashrc
```
vi .bashrc
```
- Modify your .bashrc to include the following (add to the bottom of your .bashrc):

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME="/opt/hadoop"
export SCALA_HOME="/opt/scala"
export SPARK_HOME="/opt/spark"
export PYSPARK_PYTHON="/usr/bin/python3"
export SPARK_DIST_CLASSPATH=$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*
export KAFKA_HOME="/opt/kafka"
export MAVEN="/opt/maven"

PATH="/home/ubuntu/.local/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$SCALA_HOME/bin:$MAVEN/bin:$KAFKA_HOME/bin:$PATH"
export PATH

#export CLASSPATH="$SPARK_HOME/jars/*"
export CLASSPATH="/home/ubuntu/kafka/jars/*"
#export CLASSPATH="/opt/spark/jars/aws-msk-iam-auth-2.0.3.jar:/opt/spark/jars/aws-java-sdk-bundle-1.12.667.jar:/home/ubuntu/kafka/auth-2.24.12.jar"

## Add the BROKER URL from your MSK Serverless
export broker=boot-XXXXXXXXX.c1.kafka-serverless.us-east-1.amazonaws.com:9098
export region=us-east-1
```
### Source your .bashrc to get the global variables

```
cd $BASEDIR
source .bashrc
```

## Create Kafka topic

```
cd $BASEDIR
mkdir kafka
cd kafka

# Create configuration properties file
#-----------------------------

tee client-config.properties <<EOF
# Sets up TLS for encryption and SASL for authN.
security.protocol = SASL_SSL

# Identifies the SASL mechanism to use.
sasl.mechanism = AWS_MSK_IAM

# Binds SASL client implementation.
sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required;

# Encapsulates constructing a SigV4 signature based on extracted credentials.
# The SASL client bound by "sasl.jaas.config" invokes this class.
sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF

#-----------------------------

# Creating the Kafka topic

kafka-topics.sh --create --bootstrap-server=$broker --command-config client-config.properties --replication-factor 3 --partitions 3 --topic amznbookreviews

```
