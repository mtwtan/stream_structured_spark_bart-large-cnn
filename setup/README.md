# Set up environment
** This example uses an AWS EC2 instance with NVIDIA GPU - specifically a G5.4xlarge **

## Provision an EC2 instance
- 1 G5.4xlarge launched with Ubuntu as the operating system, specifically using this AMI in the US-East-1 region: `ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20231207`

## Configure EC2 instance
- Install the NVIDIA drivers according to the [AWS documentation here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/install-nvidia-driver.html#nvidia-GRID-driver).

- For Ubuntu, some of the instructions above don't apply since Ubuntu does not use `yum`. You can follow the steps here:

### Download the necessary NVIDIA drivers
```
aws s3 cp --recursive s3://ec2-linux-nvidia-drivers/latest/ .
chmod +x NVIDIA-Linux-x86_64*.run
sudo /bin/sh ./NVIDIA-Linux-x86_64*.run
```
### Install drivers and configuration for NVIDIA
- Update packages and reboot
```
sudo apt-get update -y
sudo apt-get upgrade -y linux-aws
sudo reboot now
```
- Install gcc and configure OS
```
sudo apt-get install -y gcc make linux-headers-$(uname -r)

cat << EOF | sudo tee --append /etc/modprobe.d/blacklist.conf
blacklist vga16fb
blacklist nouveau
blacklist rivafb
blacklist nvidiafb
blacklist rivatv
EOF
```
- Add this line to /etc/default/grub
```
GRUB_CMDLINE_LINUX="rdblacklist=nouveau"
```
- Update grub
```
sudo update-grub
```
- Install AWS CLI
```
sudo apt install awscli
# verify installation
aws --version
```
- Test that NVIDIA CUDA drivers are installed:
```
nvidia-smi -q | head

==============NVSMI LOG==============

Timestamp                                 : Tue Mar 12 17:36:49 2024
Driver Version                            : 535.154.05
CUDA Version                              : 12.2

Attached GPUs                             : 1
GPU 00000000:00:1E.0
    Product Name                          : NVIDIA A10G
```

## Install Hadoop, Maven, Scala, and Spark

```
curl -O https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
curl -O https://downloads.lightbend.com/scala/2.13.12/scala-2.13.12.tgz

tar -xvf hadoop-3.3.6.tar.gz
tar -xvf spark-3.5.0-bin-hadoop3.tgz
tar -xvf scala-2.13.12.tgz

sudo mv hadoop-3.3.6 /opt/
sudo mv spark-3.5.0-bin-hadoop3 /opt/
sudo mv scala-2.13.12 /opt/

cd /opt
sudo ln -s hadoop-3.3.6 hadoop
sudo ln -s spark-3.5.0-bin-hadoop3 spark
ln -s scala-2.13.12 scala

```
## Install Java
```
sudo apt install openjdk-17-jdk
# Verify Java version
java -version
```

```
  155  curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar
  170  curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.665/aws-java-sdk-bundle-1.12.665.jar
  194  curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar
  304  curl -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar
  306  curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar
  310  curl -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar
  325  curl -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

```

```
   96  python3 -m pip install jupyterlab
  117  pip install -U torch tensorflow transformers bitsandbytes>=0.39.0 accelerate
  120  python3 -m pip install pyspark
  131  python3 -m pip install boto3
  245  python3 -m pip install faker
  365  python3 -m pip install delta-spark==3.1.0
  421  python3 -m pip install datasets
  423  python3 -m pip install tensorrt
  424  python3 -m pip install chunkipy
```
