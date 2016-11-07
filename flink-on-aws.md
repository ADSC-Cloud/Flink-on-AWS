Flink On AWS
=======================

chmod 400 keypair.pem
If you do not set permissions on the .pem file, you will receive an error indicating that your key file is unprotected and the key will be rejected. To connect, you only need to set permissions on the key pair private key file the first time you use it.

export HADOOP_CONF_DIR=/etc/hadoop/conf
if not set, 
Diagnostics: File file:/home/hadoop/.flink/application_1476068848157_0007/flink-
conf.yaml does not exist
java.io.FileNotFoundException: File file:/home/hadoop/.flink/application_1476068
848157_0007/flink-conf.yaml does not exist


get slave ip
hdfs dfsadmin -report | grep Name: | cut -d':' -f2 | sed 's/ //g'
then `ssh slave`
172.31.19.53
172.31.28.10
172.31.28.11

# Buffer timeout

By default data points are not transferred on the network one-by-one, which would cause unnecessary network traffic, but are buffered in the output buffers. The size of the output buffers can be set in the Flink config files. While this method is good for optimizing throughput, it can cause latency issues when the incoming stream is not fast enough. To tackle this issue the user can call env.setBufferTimeout(timeoutMillis) on the execution environment (or on individual operators) to set a maximum wait time for the buffers to fill up. After this time the buffers are flushed automatically even if they are not full. The default value for this timeout is 100ms which should be appropriate for most use-cases.

Usage:

	LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
	env.setBufferTimeout(timeoutMillis);
	env.genereateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);

To maximise the throughput the user can call .setBufferTimeout(-1) which will remove the timeout and buffers will only be flushed when they are full. To minimise latency, set the timeout to a value close to 0 (fro example 5 or 10 ms). Theoretically a buffer timeout of 0 will cause all outputs to be flushed when produced, but this setting should be avoided because it can cause severe performance degradation.


## EC2

### FLINK_SSH_OPT
in conf/flink-conf.yaml, add
env.ssh.opts: "-i ~/.ssh/flink-cluster.pem"

or

cat ~/.ssh/id_rsa.pub | ssh ubuntu@123.45.56.78 "mkdir -p ~/.ssh && cat >>  ~/.ssh/authorized_keys"

### install java8

sudo add-apt-repository ppa:webupd8team/java; sudo apt-get update; sudo apt-get install oracle-java8-installer

### cluster setup
custom port 6000-60000


### rescaling
http://www.slideshare.net/tillrohrmann/dynamic-scaling-how-apache-flink-adapts-to-changing-workloads

### auto-backpressure(from Kafka)
http://data-artisans.com/kafka-flink-a-practical-how-to/

