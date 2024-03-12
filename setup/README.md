# Set up environment
** This example uses an AWS EC2 instance with NVIDIA GPU - specifically a G5.4xlarge **

## Provision an EC2 instance
- 1 G5.4xlarge launched with Ubuntu as the operating system, specifically using this AMI in the US-East-1 region: `ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-20231207`

## Configure EC2 instance


```
   11  blacklist nvidiafb
   21  aws s3 cp --recursive s3://ec2-linux-nvidia-drivers/latest/ .
   23  chmod +x NVIDIA-Linux-x86_64*.run
   25  sudo /bin/sh ./NVIDIA-Linux-x86_64*.run
   26  nvidia-smi
   36  nvidia-smi -q | head
```

```
 3  sudo apt-get update -y
    4  sudo apt-get upgrade -y linux-aws
    5  sudo reboot now
    6  sudo apt-get install -y gcc make linux-headers-$(uname -r)
    7  cat << EOF | sudo tee --append /etc/modprobe.d/blacklist.conf
    8  blacklist vga16fb
    9  blacklist nouveau
   10  blacklist rivafb
   11  blacklist nvidiafb
   12  blacklist rivatv
   13  EOF
   14  GRUB_CMDLINE_LINUX="rdblacklist=nouveau"
   15  vi /etc/default/grub
   16  sudo vi /etc/default/grub
   17  sudo update-grub
   18  aws --version
   19  sudo apt install awscli
   20  aws
   21  aws s3 cp --recursive s3://ec2-linux-nvidia-drivers/latest/ .
   22  ls -l
   23  chmod +x NVIDIA-Linux-x86_64*.run
   24  ls -l
   25  sudo /bin/sh ./NVIDIA-Linux-x86_64*.run
   26  nvidia-smi

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
