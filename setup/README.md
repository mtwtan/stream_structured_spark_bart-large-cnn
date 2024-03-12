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


```
   37  curl -O https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
   38  ls -l
   39  curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
   40  ls -l
   41  tar -xvf hadoop-3.3.6.tar.gz
   42  ls -l
   43  tar -xvf spark-3.5.0-bin-hadoop3.tgz
   44  ls -l
   45  sudo mv hadoop-3.3.6 /opt/
   46  ls -l /opt
   47  sudo mv spark-3.5.0-bin-hadoop3 /opt/
   48  ls -l
   49  df -h
   50  cd /opt
   51  ls -l
   52  sudo ln -s hadoop-3.3.6 hadoop
   53  sudo ln -s spark-3.5.0-bin-hadoop3 spark

```

```
   55  java -version
   56  sudo apt search openjdk-17
   57  sudo apt search openjdk-17 | grep openjdk-17
   58  sudo apt install openjdk-17-jdk
   59  sudo apt search openjdk-17 | grep openjdk-17
   60  java -version

```

```
   67  curl -O https://downloads.lightbend.com/scala/2.13.12/scala-2.13.12.tgz
   68  ls -l
   69  tar -xvf scala-2.13.12.tgz
   70  tar -xvf scala-2.13.12
   71  ls -l
   72  sudo mv scala-2.13.12 /opt/
   73  ls -l
   74  df -h
   75  ls -l
   76  vi .bashrc
   77  cd /opt
   78  ls -l
   79  ln -s scala-2.13.12 scala
   80  sudo ln -s scala-2.13.12 scala

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
