#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd`

. "$bin"/hdfs-config.sh

function print_usage(){
  echo "Usage: ezdeploy \"<ZOOKEEPERLIST>\""
}

if [ $# = 0 ]; then
  print_usage
  exit
fi

ZKLIST=$1

# update zookeeper list in conf files
echo " >> update zookeeper list in conf files"

ZKTEMP="127.0.0.1:2181"
sed -i "s/$ZKTEMP/$ZKLIST/g" ${HADOOP_CONF_DIR}/hdfs-site.xml
sed -i "s/$ZKTEMP/$ZKLIST/g" ${HADOOP_CONF_DIR}/distributed-server-site.xml

# update namenode address in conf files
echo " >> update namenode address in conf files"

HOSTNAME=`hostname`
NNTEMP="hdfs:\/\/127.0.0.1:51199"
NNADDR="hdfs:\/\/$HOSTNAME:51199"
NNLIST=$NNADDR

while read host
do
 NNHOST="hdfs:\/\/${host}:51199"
 NNLIST="${NNLIST},${NNHOST}"
 cp ${HADOOP_CONF_DIR}/core-site.xml /tmp/core-site.xml.${host}
 sed -i "s/${NNTEMP}/${NNHOST}/g" /tmp/core-site.xml.${host}
done < ${HADOOP_CONF_DIR}/masters

sed -i "s/$NNTEMP/$NNADDR/g" ${HADOOP_CONF_DIR}/core-site.xml
sed -i "s/$NNTEMP/$NNLIST/g" ${HADOOP_CONF_DIR}/hdfs-site.xml

# send files to target servers
echo " >> send files to target servers"
HADOOP_PATH=`cd ${HADOOP_PREFIX};pwd`

while read host
do
 ssh -n $host "rm -rf ${HADOOP_PATH};mkdir ${HADOOP_PATH}"
 rsync -az ${HADOOP_PATH} $host:~/
 rsync -az /tmp/core-site.xml.${host} $host:$HADOOP_PATH/conf/core-site.xml
 rm -rf /tmp/core-site.xml.${host}
done < ${HADOOP_CONF_DIR}/masters

DNLIST=""
while read host
do
 DNLIST="${DNLIST},$host"
 ssh -n $host "rm -rf ${HADOOP_PATH};mkdir ${HADOOP_PATH}"
 rsync -az ${HADOOP_PATH} $host:~/
done < ${HADOOP_CONF_DIR}/slaves

NDLIST="${NNLIST},${DNLIST}"
ZKARRAY=`echo ${ZKLIST} | awk -F ', ' '{print $0} ' | sed "s/,/ /g "`
zknum=`echo $ZKLIST | awk -F',' '{print NF-1}'`
cnt=1
echo "" >> ${HADOOP_CONF_DIR}/zoo.cfg
for zkaddr in $ZKARRAY 
do 
  zkhost=`echo $zkaddr | sed "s/:.*//g"`
  deployed=`echo $NDLIST |grep ${zkhost}`
  # copy files to zookeeper cluster
  if [ -z "$deployed" ]; then
    ssh -n $zkhost "rm -rf ${HADOOP_PATH};mkdir ${HADOOP_PATH}"
    rsync -az ${HADOOP_PATH} $zkhost:~/
  fi
  # configure zookeeper cluster
  if [ "$zknum" -ne 0 ]; then
    echo "server.${cnt}=${zkhost}:2888:3888" >> ${HADOOP_CONF_DIR}/zoo.cfg
    let "cnt++"
  fi
done

# start zookeeper
echo " >> start zookeeper"
cnt=1
dataDir=`cat ${HADOOP_CONF_DIR}/zoo.cfg |grep "dataDir"|sed 's/.*=//g'`
dataLogDir=`cat ${HADOOP_CONF_DIR}/zoo.cfg |grep "dataLogDir"|sed 's/.*=//g'`
for zkaddr in $ZKARRAY 
do 
  zkhost=`echo $zkaddr | sed "s/:.*//g"`
  if [ "$zknum" -ne 0 ]; then
    rsync -az ${HADOOP_CONF_DIR}/zoo.cfg $zkhost:${HADOOP_PATH}/conf/zoo.cfg
    ssh -n $zkhost "cd ${HADOOP_PATH};mkdir ${dataDir};mkdir ${dataLogDir};echo ${cnt} > ${dataDir}/myid"
    let "cnt++"
  else
    ssh -n $zkhost "cd ${HADOOP_PATH};mkdir ${dataDir};mkdir ${dataLogDir}"
  fi
  ssh -n $zkhost "cd ${HADOOP_PATH}; bin/zkServer.sh start &>/dev/null"
done

# let zookeeper cluster has enough time for startup
sleep 2

# start state manager
echo " >> start state manager"
cd ${HADOOP_PATH}; bin/dsserver.sh format
while read host                                                                                                   
do
 ssh -n $host "cd ${HADOOP_PATH}; bin/dsserver.sh format &>/dev/null"
done < ${HADOOP_CONF_DIR}/masters

# start dfs cluster
echo " >> start dfs cluster"
while true
do
 isok=`$bin/dsclient.sh getServers |grep "Type=MASTER"`
 if [ -n "$isok" ] ; then
  $bin/start-dfs.sh
  break
 fi
 echo "   ..waiting for state manager startup.."
 sleep 5
done

# done
echo " >> congratulations, adfs deployment is done!"
