/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.database;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.BeforeClass;
import org.junit.Test;

import com.taobao.adfs.database.MysqlServerController;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 * @created 2011-06-28
 */
public class MysqlServerControllerTest {
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Utilities.delete(new File("target/test" + MysqlServerControllerTest.class.getSimpleName()));
  }

  /**
   * need to install mysql, xtrabackup and innobackupex in $PATH
   */
  @Test
  public void testFormatAndGetAndSetData() throws IOException {
    String osType = Utilities.getOsType();
    if (osType.isEmpty()) System.out.println("WARN: unsupported operation system");

    // test format data
    Configuration conf1 = new Configuration(false);
    conf1.set("mysql.server.bin.path", "src/main/tool/mysql-" + osType + "/bin");
    conf1.set("mysql.server.data.path", "target/test" + getClass().getSimpleName() + "/dataPath1");
    conf1.set("mysql.server.backup.data.path", "target/test" + getClass().getSimpleName() + "/dataPath2");
    if (new File(conf1.get("mysql.server.data.path") + "/mysqld.sock").getAbsolutePath().length() > 107) {
      conf1.set("mysql.server.data.path", "/tmp/test" + getClass().getSimpleName() + "/dataPath1");
      conf1.set("mysql.server.backup.data.path", "/tmp/test" + getClass().getSimpleName() + "/dataPath2");
    }
    conf1
        .set(
            "mysql.server.database.create.sql.statement",
            "create database if not exists testDatabase; use `testDatabase`; create table if not exists testTable(id int,\nnum int);");
    conf1.set("mysql.server.conf.mysqld.port", "31230");
    conf1.set("mysql.server.conf.mysqld.loose_handlersocket_port", "31231");
    conf1.set("mysql.server.conf.mysqld.loose_handlersocket_port_wr", "31232");
    conf1.set("mysql.server.restore", "false");
    Configuration conf2 = new Configuration(false);
    conf2.set("mysql.server.bin.path", conf1.get("mysql.server.bin.path"));
    conf2.set("mysql.server.data.path", conf1.get("mysql.server.backup.data.path"));
    conf2.set("mysql.server.conf.mysqld.port", "41230");
    conf2.set("mysql.server.conf.mysqld.loose_handlersocket_port", "41231");
    conf2.set("mysql.server.conf.mysqld.loose_handlersocket_port_wr", "41232");
    conf2.set("mysql.server.restore", "false");

    String mysqlServerPid1 = new MysqlServerController().getServerPid(conf1);
    new MysqlServerController().stopServer(conf1);
    new MysqlServerController().stopServer(conf2);
    Utilities.delete(new File(conf1.get("mysql.server.data.path")).getParentFile());

    new MysqlServerController().formatData(conf1);
    String mysqlDataPath = conf1.get("mysql.server.data.path");
    assertThat(new File(mysqlDataPath).isDirectory(), is(true));
    assertThat(new File(mysqlDataPath + "/mysql").isDirectory(), is(true));
    assertThat(new File(mysqlDataPath + "/testDatabase").isDirectory(), is(true));
    assertThat(new File(mysqlDataPath + "/testDatabase/testTable.frm").isFile(), is(true));

    // test get data
    ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();
    String mysqlDataPathRemote = new MysqlServerController().getData(conf1, reentrantReadWriteLock.writeLock());
    // check whether data is backuped
    Set<String> backupFiles = new HashSet<String>();
    for (String file : new File(mysqlDataPathRemote).list()) {
      backupFiles.add(file);
    }
    assertThat(reentrantReadWriteLock.writeLock().isHeldByCurrentThread(), is(true));
    assertThat(backupFiles.size() > 0, is(true));
    assertThat(backupFiles.contains("backup-my.cnf"), is(true));
    assertThat(backupFiles.contains("ibdata1"), is(true));
    assertThat(backupFiles.contains("mysql"), is(true));
    assertThat(backupFiles.contains("xtrabackup_checkpoints"), is(true));
    assertThat(backupFiles.contains("xtrabackup_logfile"), is(true));

    // test set data
    String mysqlServerPid2 = new MysqlServerController().getServerPid(conf2);

    String mysqlDataPathLocal = new MysqlServerController().setData(conf2);
    // check whether data is applied
    backupFiles = new HashSet<String>();
    for (String file : new File(mysqlDataPathLocal).list()) {
      backupFiles.add(file);
    }
    assertThat(backupFiles.size() > 0, is(true));
    assertThat(backupFiles.contains("ibdata1"), is(true));
    assertThat(backupFiles.contains("ib_logfile0"), is(true));
    assertThat(backupFiles.contains("mysql"), is(true));
    assertThat(backupFiles.contains("xtrabackup_checkpoints"), is(true));
    assertThat(backupFiles.contains("xtrabackup_logfile"), is(true));
    // check mysql server can start
    new MysqlServerController().startServer(conf2);
    assertThat(new MysqlServerController().getServerPid(conf2).isEmpty(), is(false));

    // restore mysql server status before test
    if (mysqlServerPid1.isEmpty()) new MysqlServerController().stopServer(conf1);
    if (mysqlServerPid2.isEmpty()) new MysqlServerController().stopServer(conf2);
  }

  @Test
  public void testMoveData() throws IOException, InterruptedException {
    File dataPath = new File("target/test" + getClass().getSimpleName() + "/testMoveData/dataPathOfTestMoveData");
    dataPath.mkdirs();
    new MysqlServerController().moveData(dataPath.getAbsolutePath(), Long.MAX_VALUE);
    assertThat(dataPath.exists(), is(false));
    assertThat(dataPath.getParentFile().list().length == 1, is(true));
    dataPath.mkdirs();
    new MysqlServerController().moveData(dataPath.getAbsolutePath(), Long.MAX_VALUE);
    assertThat(dataPath.exists(), is(false));
    assertThat(dataPath.getParentFile().list().length == 2, is(true));
    Thread.sleep(1000);
    dataPath.mkdirs();
    new MysqlServerController().moveData(dataPath.getAbsolutePath(), 500);
    assertThat(dataPath.exists(), is(false));
    assertThat(dataPath.getParentFile().list().length == 1, is(true));
  }
}
