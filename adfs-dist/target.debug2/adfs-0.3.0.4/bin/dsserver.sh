#usage: 
#默认会读取./*-default.xml和./*-site.xml, 如果设定了-Dconf=$CONF_PATH，则会读取$CONF_PATH/*-default.xml和$CONF_PATH/*-site.xml

pwd=`pwd`
shell=$0
if [ ! -f $shell ]
then
 shell=`which $shell`
fi
bin=`readlink -f $shell`
bin=`dirname $bin`
bin=`cd "$bin"; pwd`
cd $pwd

command=$1
extraParameter=""

shift

for distributedJar in `ls $bin/../modules/adfs-distributed-*.jar`
 do classpathForDistributedServer=`echo $distributedJar:$classpathForDistributedServer`;
done
classpath="$classpathForDistributedServer:$bin/../modules/*:$bin/../lib/*:./"

case "$command" in
start)
 java -cp $classpath $DS_OPTS com.taobao.adfs.distributed.DistributedServer $* "$extraParameter"&
 ;;
stop)
 java -cp $classpath $DS_OPTS com.taobao.adfs.distributed.DistributedServer $* "$extraParameter" "-Ddistributed.server.stop=true"
 ;;
format)
 java -cp $classpath $DS_OPTS com.taobao.adfs.distributed.DistributedServer $* "$extraParameter" "-Ddistributed.data.format=true"&
 ;;
performance)
 java -cp $classpath $DS_OPTS com.taobao.adfs.distributed.DistributedPerformance $* "$extraParameter"
 ;;
mysqlTest)
 java -cp $classpath com.taobao.adfs.database.MysqlClientPermanceTest $* "$extraParameter"
 ;;
hsTest)
 java -cp $classpath com.taobao.adfs.database.HandlerSocketClientPermanceTest $* "$extraParameter"
 ;;
tdhsTest)
 java -cp $classpath com.taobao.adfs.database.TdhSocketClientPermanceTest $* "$extraParameter"
 ;;
iopsTest)
 java -cp $classpath com.taobao.adfs.performance.test.FsyncTest $* "$extraParameter"
 ;;
confTest)
 java -cp $classpath com.taobao.adfs.performance.test.ConfTest $* "$extraParameter"
 ;;
tdhsTest)
 java -cp $classpath com.taobao.adfs.database.TdhSocketClientPermanceTest $* "$extraParameter"
 ;;
innodbTest)
 java -cp $classpath com.taobao.adfs.database.InnodbJniClientPermanceTest $* "$extraParameter"
 ;;
iosimFileCreator)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.FileCreator $* "$extraParameter"
 ;;
iosimBlockAllocator)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.BlockAllocator $* "$extraParameter"
 ;;
iosimDatanodeReg)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.DatanodeReg $* "$extraParameter"
 ;;
iosimBlockReceiver)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.BlockReceiver $* "$extraParameter"
 ;;
iosimBlockSynchronization)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.BlockSynchronization $* "$extraParameter"
 ;;
iosimFileCreator)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.FileCreator $* "$extraParameter"
 ;;
iosimFileCompleted)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.FileCompleted $* "$extraParameter"
 ;;
iosimFileInfoGet)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.FileInfoGet $* "$extraParameter"
 ;;
iosimFileListing)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.FileListing $* "$extraParameter"
 ;;
iosimFileOpen)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.FileOpen $* "$extraParameter"
 ;;
iosimFileRename)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.FileRename $* "$extraParameter"
 ;;
iosimFileReplication)
 java -cp $classpath com.taobao.adfs.iosimulator.scenarios.FileReplication $* "$extraParameter"
 ;;
profile)
 command="$bin/../../../tool/btrace/bin/btrace -cp $classpath `jps|grep DistributedServer|grep -v grep|awk '{print $1}'` $bin/../../../src/test/java/com/taobao/adfs/distributed/DistributedProfiler.java"
 echo "$command"
 bash -c "$command"
 ;;
*)
 echo "usage: server.sh start|stop|format|performance|mysqlTest|hsTest|tdhsTest|iopsTest|profile parameters"
 exit 1
esac

