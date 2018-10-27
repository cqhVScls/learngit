1、在linux中创建一个目录
mkdir /home/hadoop/oozie/job/mr
a、在该目录下创建job.properties
cd /home/hadoop/oozie/job/mr
vi  ./job.properties
内容如下：
nameNode=hdfs://hadoop01:9000
jobTracker=hadoop01:8032
queueName=default
anlastic_path=/analstic/job/oozie
run_date=2018-05-31
oozie.use.system.libpath=true
oozie.libpath=${nameNode}${anlastic_path}/hbaselib/

oozie.wf.application.path=${nameNode}${anlastic_path}/mr

b、在该目录下创建workflow.xml
vi  ./workflow.xml
内容省略：


2、在hdfs中创建目录 /analstic/job/oozie/hbaselib
hdfs dfs -mkdir -p hdfs://hadoop01:9000/analstic/job/oozie/hbaselib
把linux中的hbase安装目录下的lib目录中的所有的jar包上传到hdfs中的hbaselib目录
hdfs dfs -put $HBASE_HOME/lib/*.jar hdfs://hadoop01:9000/analstic/job/oozie/hbaselib

3、将linux中的/home/hadoop/oozie/job/mr目录下的刚创建的workflow.xml文件上传hdfs的/analstic/job/oozie/mr目录中
hdfs dfs -mkdir /analstic/job/oozie/mr
hdfs dfs -put ./workflow.xml /analstic/job/oozie/mr

4、再在hdfs的/analstic/job/oozie/mr目录中创建一个lib目录来存储java的jar包
hdfs dfs -mkdir /analstic/job/oozie/mr/lib

5、将java中打的jar包上传到/analstic/job/oozie/mr/lib目录中
hdfs dfs -put /home/GP1704_AnlaysticLog-0.1.jar /analstic/job/oozie/mr/lib

6、运行oozie的wf
oozie job -oozie http://hadoop01:11000/oozie -config ./job.properties -run