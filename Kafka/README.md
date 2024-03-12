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
- Modify your .bashrc to include the following:

```
```

```
