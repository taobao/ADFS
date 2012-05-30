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

for distributedJar in `ls $bin/../modules/adfs-distributed-*.jar`
 do classpathForDistributedServer=`echo $distributedJar:$classpathForDistributedServer`;
done
classpath="$classpathForDistributedServer:$bin/../modules/*:$bin/../lib/*:./"

java -cp $classpath $DS_OPTS com.taobao.adfs.distributed.DistributedShell $*
