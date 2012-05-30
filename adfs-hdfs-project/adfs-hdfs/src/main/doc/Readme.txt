ExampleServer快速使用步骤:

1.启动zookeeper服务器(单机模式)
命令：
cd $ZOOKEEPER_HOME/conf/; echo "dataDir=./data">zoo.cfg; echo "clientPort=2181">>zoo.cfg; ../bin/zkServer.sh start
说明:
zookeeper服务器将会启动在localhost:2181

2.编译distributed项目
命令：
cd $DISTRIBUTED_HOME; mvn clean install
说明:
可以看到生成的文件夹"$DISTRIBUTED_HOME/target/dist"

3.启动example服务器
命令：
cd $DISTRIBUTED_HOME/target/dist; bash bin/startupServer.sh
说明:
Example服务器默认启动在localhost:50000，可以通过bash bin/startupServer.sh -Ddistributed.server.name=41000启动第二个服务器

4.使用命令行写数据到服务器
命令：
cd $DISTRIBUTED_HOME/target/dist; bash bin/shell.sh write 1234567890
说明:
终端会回显"return : 1234567890"

5.使用命令行读出写入的数据
命令：
cd $DISTRIBUTED_HOME/target/dist; bash bin/shell.sh read
说明:
终端会回显"return : 1234567890"

6.关闭服务器
命令：
cd $DISTRIBUTED_HOME/target/dist; bash bin/shell.sh localhost:50000 stop
说明:
关闭localhost:51000上的服务器使用命令"bin/shell.sh localhost:51000 stop"

7.查看shell支持的命令
bash bin/shell.sh