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

import com.taobao.adfs.util.Utilities;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 * @created 2011-06-28
 */
public class MysqlServerController {
  public static final Logger logger = LoggerFactory.getLogger(MysqlServerController.class);
  public static final String mysqlConfKeyPrefix = "mysql.server.conf.";

  /**
   * need to avoid timeout of sub-process. note: innobackupex has been modified by jiwan@taobao.com
   */
  public String getData(Configuration conf, Lock writeLock) throws IOException {
    // get settings
    setMysqlDefaultConf(conf);
    String dataPathLocal = Utilities.getNormalPath(conf.get("mysql.server.data.path", "."));
    String dataPathRemote = Utilities.getNormalPath(conf.get("mysql.server.backup.data.path", "."));
    String remoteHost = conf.get("mysql.server.backup.host", "localhost");
    String mysqlConfPathLocal = dataPathLocal + "/my.cnf";
    Utilities.mkdirsInRemote(remoteHost, dataPathRemote, true);

    String mysqlServerPid = startServer(conf);

    // generate command line
    // note: innobackupex has been modified by jiwan@taobao.com
    String cmdLine = "innobackupex";
    cmdLine += " --user=root";
    cmdLine += " --password=" + conf.get("mysql.server.password", "root");
    cmdLine += " --defaults-file=" + mysqlConfPathLocal;
    cmdLine += " --socket=" + conf.get(mysqlConfKeyPrefix + "mysqld.socket");
    cmdLine += " --no-lock";// it will save 2s for no needing to unlock tables
    cmdLine += " --suspend-at-end";
    cmdLine += " --stream=tar";
    cmdLine += " " + dataPathLocal;
    cmdLine += "|gzip|";
    if (Utilities.isLocalHost(remoteHost)) cmdLine += " bash -c";
    else cmdLine += "ssh " + remoteHost;
    if (conf.getBoolean("mysql.server.backup.decompress", true)) cmdLine += " \"tar -zixC " + dataPathRemote + "\"";
    else cmdLine += " cat >" + dataPathRemote + "/backup.tar.gz";
    Utilities.logInfo(logger, "Command=", cmdLine);

    // run command line
    Process process =
        Utilities.runCommand(new String[] { "/bin/bash", "-c", cmdLine }, conf.get("mysql.server.bin.path"), null);

    // read the stderr stream get backup status and write the stdin stream to control it.
    // 0.innobackupex copied all data (not stop monitor the log)
    // 1.innobackupex suspends
    // 2.HotBackup finds innobackupex has suspended
    // 3.HotBackup notify master server to stops write service
    // 4.HotBackup notify master server to forward write requests to slave server
    // 5.HotBackup signal innobakupex to continue backup work
    // 6.HotBackup resumes and exits
    // 7.HotBackup notify master server to restart write service
    // 8.all write requests to master server be forwarded to the slave server
    // 9.slave server will apply the backup data and new requests
    BufferedWriter stdInputWriter = null;
    BufferedReader stdErrorReader = null;

    String line = null;
    boolean backupSuccessful = false;
    try {
      stdInputWriter = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
      stdErrorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

      while ((line = stdErrorReader.readLine()) != null) {
        Utilities.logInfo(logger, line);
        if (line.contains("WAIT_UNTIL_PARENT_PROCESS_SIGNAL")) {
          Utilities.logInfo(logger, "prepare to block write request for backuping mysql server data");
          writeLock.lock();
          Utilities.logInfo(logger, "already blocked write request for backuping mysql server data");
          // notify xtrabackup to complete the backup
          stdInputWriter.append("SIGNAL_CHILD_PROCESS_TO_CONTINUE\n");
          stdInputWriter.flush();
        }
        if (line.contains("completed OK!") && !line.contains("prints")) {
          backupSuccessful = true;
          Utilities.logInfo(logger, "complete backup.");
          break;
        }
      }
    } catch (IOException e) {
      throw e;
    } finally {
      if (stdInputWriter != null) stdInputWriter.close();
      if (stdErrorReader != null) stdErrorReader.close();
      process.destroy();
    }

    // wait until files are created
    for (int i = 0; i < 100; ++i) {
      try {
        Utilities.lsInRemote(remoteHost, dataPathRemote + "/xtrabackup_checkpoints");
        Utilities.lsInRemote(remoteHost, dataPathRemote + "/xtrabackup_logfile");
        Utilities.lsInRemote(remoteHost, dataPathRemote + "/ibdata1");
        break;
      } catch (Throwable t) {
        if (i == 99) backupSuccessful = false;
        else Utilities.sleepAndProcessInterruptedException(100, logger);
      }
    }

    if (!backupSuccessful) throw new IOException("fail to backup mysql server data");

    if (mysqlServerPid.isEmpty() && conf.getBoolean("mysql.server.restore", false)) stopServer(conf);

    return dataPathRemote;
  }

  /**
   * need to avoid timeout of sub-process. note: innobackupex has been modified by jiwan@taobao.com
   */
  public String setData(Configuration conf) throws IOException {
    // get settings
    saveMysqlConf(conf);
    String dataPathLocal = Utilities.getNormalPath(conf.get("mysql.server.data.path", "."));
    String mysqlConfPathLocal = dataPathLocal + "/my.cnf";

    String mysqlServerPid = stopServer(conf);

    // generate command line
    // example: innobackupex --apply-log --user=root --password=root --defaults-file=/etc/mysql/my.cnf /var/lib/mysql
    // note: innobackupex-1.5.1 has been modified by jiwan@taobao.com
    String cmdLine = "innobackupex";
    cmdLine += " --apply-log";
    cmdLine += " --user=root";
    cmdLine += " --password=" + conf.get("mysql.server.password", "root");
    cmdLine += " --defaults-file=" + mysqlConfPathLocal;
    cmdLine += " --socket=" + getMysqlConf(conf, "mysqld.socket");
    cmdLine += " " + dataPathLocal;
    Utilities.logInfo(logger, "Command=", cmdLine);

    // run command line
    Process process =
        Utilities.runCommand(new String[] { "/bin/bash", "-c", cmdLine }, conf.get("mysql.server.bin.path"), null);

    BufferedWriter stdInputWriter = null;
    BufferedReader stdErrorReader = null;

    String line = null;
    boolean restoreSuccessful = false;
    try {
      stdInputWriter = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
      stdErrorReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      while ((line = stdErrorReader.readLine()) != null) {
        Utilities.logInfo(logger, line);
        if (line.contains("completed OK!") && !line.contains("prints")) {
          restoreSuccessful = true;
          Utilities.logInfo(logger, "complete apply log.");
          break;
        }
      }
    } catch (IOException e) {
      throw e;
    } finally {
      if (stdInputWriter != null) stdInputWriter.close();
      if (stdErrorReader != null) stdErrorReader.close();
      process.destroy();
    }

    if (!restoreSuccessful) throw new IOException("fail to restore database");

    if (!mysqlServerPid.isEmpty() && conf.getBoolean("mysql.server.restore", false)) startServer(conf);

    return dataPathLocal;
  }

  public String moveData(String pathOfMysqlData, long expireTimeOfOldMysqlData) throws IOException {
    File fileOfMysqlData = new File(pathOfMysqlData).getAbsoluteFile();
    if (!fileOfMysqlData.exists()) {
      Utilities.logInfo(logger, "no need to move not existed path=", pathOfMysqlData);
      return null;
    }
    try {
      String newPathOfMysqlData =
          pathOfMysqlData + "-" + Utilities.longTimeToStringTime(System.currentTimeMillis(), "");
      fileOfMysqlData.renameTo(new File(newPathOfMysqlData));
      File parentFileOfMysqlData = fileOfMysqlData.getParentFile();
      for (File oldFileOfMysqlData : parentFileOfMysqlData.listFiles()) {
        if (oldFileOfMysqlData.getName().startsWith(fileOfMysqlData.getName())) {
          String timeString = oldFileOfMysqlData.getName().substring(fileOfMysqlData.getName().length());
          if (!timeString.startsWith("-")) continue;
          timeString = timeString.substring(1);
          long time = Utilities.stringTimeToLongTime(timeString, "");
          if (System.currentTimeMillis() - time > expireTimeOfOldMysqlData) {
            Utilities.delete(oldFileOfMysqlData);
            Utilities.logInfo(logger, "delete ", oldFileOfMysqlData);
          }
        }
      }
      Utilities.logInfo(logger, "move ", pathOfMysqlData, " to ", newPathOfMysqlData);
      return newPathOfMysqlData;
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  public String backupData(Configuration conf) throws IOException {
    String mysqlDataPath = Utilities.getNormalPath(conf.get("mysql.server.data.path", "."));
    long expireTimeOfOldMysqlData = conf.getLong("mysql.server.data.path.old.expire.time", 30L * 24 * 3600 * 1000);
    return moveData(mysqlDataPath, expireTimeOfOldMysqlData);
  }

  public void formatData(Configuration conf) throws IOException {
    Utilities.logInfo(logger, "mysql server is formatting");
    setMysqlDefaultConf(conf);
    // stop mysql server and initialize data
    String mysqlServerPid = stopServer(conf);
    backupData(conf);
    String mysqlDataPath = Utilities.getNormalPath(conf.get("mysql.server.data.path", "."));
    Utilities.mkdirs(mysqlDataPath, true);
    String commandForCreateMysqlData = "mysql_install_db";
    commandForCreateMysqlData += " --force";
    commandForCreateMysqlData += " --no-defaults";
    commandForCreateMysqlData += " --basedir=" + getMysqlConf(conf, "mysqld.basedir");
    commandForCreateMysqlData += " --datadir=" + mysqlDataPath;
    Utilities.logInfo(logger, "mysql server is installing new data, command=", commandForCreateMysqlData);
    Utilities.runCommand(commandForCreateMysqlData, 0, conf.get("mysql.server.bin.path"), null);
    Utilities.logInfo(logger, "mysql server has installed new data");

    // start mysql server and set access control
    startServer(conf);
    String commandForSetMysqlAccess = "mysql -uroot";
    commandForSetMysqlAccess += " --socket=" + getMysqlConf(conf, "mysqld.socket");
    // commandForSetMysqlServerPassword += " password '" + conf.get("mysql.password", "root") + "'";
    commandForSetMysqlAccess += " --execute=\"";
    commandForSetMysqlAccess +=
        "use mysql;delete from user;grant all privileges on *.* to 'root'@'%' identified by 'root';flush privileges;";
    commandForSetMysqlAccess += "\"";
    Utilities.logInfo(logger, "mysql server is setting privileges, command=", commandForSetMysqlAccess);
    Utilities.runCommand(commandForSetMysqlAccess, 0, conf.get("mysql.server.bin.path"), conf
        .get("mysql.server.bin.path")
        + "/../lib/mysql");
    Utilities.logInfo(logger, "mysql server has set privileges");

    // create database
    try {
      createDatabase(conf);
    } catch (Throwable t) {
      int retryIndex = conf.getInt("mysql.server.format.retry.index", 0);
      if (retryIndex >= conf.getInt("mysql.server.format.retry.max", 3)) throw new IOException(t);
      conf.setInt("mysql.server.format.retry.index", ++retryIndex);
      Utilities.logError(logger, "mysql server fails to create database, retryIndex=", retryIndex, t);
      formatData(conf);
      return;
    }

    // restore mysql server status before format
    if (mysqlServerPid.isEmpty() && conf.getBoolean("mysql.server.restore", false)) stopServer(conf);
    Utilities.logInfo(logger, "mysql server is formatted");
  }

  public void createDatabase(Configuration conf) throws IOException {
    setMysqlDefaultConf(conf);
    String commandForCreateDatabase = "mysql -uroot -p" + conf.get("mysql.server.password", "root");
    commandForCreateDatabase += " --socket=" + getMysqlConf(conf, "mysqld.socket");
    commandForCreateDatabase += " --execute=\"";
    if (!conf.get("mysql.server.database.create.sql.statement", "").isEmpty()) {
      commandForCreateDatabase += conf.get("mysql.server.database.create.sql.statement").replaceAll("[\n\r]", "");
    }
    commandForCreateDatabase += "\"";
    commandForCreateDatabase = commandForCreateDatabase.replaceAll("`", "\\\\`");
    Utilities.logInfo(logger, "mysql server is creating database(s), command=", commandForCreateDatabase);
    Utilities.runCommand(commandForCreateDatabase, 0, conf.get("mysql.server.bin.path"), conf
        .get("mysql.server.bin.path")
        + "/../lib/mysql");
    Utilities.logInfo(logger, "mysql server has created database(s)");
  }

  /**
   * @return mysql server pid
   */
  public String startServer(Configuration conf) throws IOException {
    // start mysql server
    setMysqlDefaultConf(conf);
    String mysqlConfPath = Utilities.getNormalPath(conf.get("mysql.server.data.path", ".")) + "/my.cnf";
    String mysqlServerPid = getServerPid(conf);
    if (!getServerPid(conf).isEmpty()) return mysqlServerPid;
    saveMysqlConf(conf);
    String commandForStartMysqld = "mysqld --defaults-file=" + mysqlConfPath;
    Utilities.logInfo(logger, "mysql server is starting with user=", getMysqlConf(conf, "mysqld.user"), ", command=",
        commandForStartMysqld);
    Utilities.runCommand(commandForStartMysqld, null, conf.get("mysql.server.bin.path"), null, false);
    for (int i = 0; i < 60; ++i) {
      mysqlServerPid = getServerPid(conf);
      if (!mysqlServerPid.isEmpty() && getServerListenAddresses(conf).size() > 1) {
        Utilities.logInfo(logger, "mysql server has started");
        return mysqlServerPid;
      }
      Utilities.sleepAndProcessInterruptedException(1000, logger);
    }
    throw new IOException("fail to start mysql with command=" + commandForStartMysqld);
  }

  public void saveMysqlConf(Configuration conf) throws IOException {
    setMysqlDefaultConf(conf);

    // get mysql configuration
    Map<String, Map<String, String>> mysqlConfMap = new HashMap<String, Map<String, String>>();
    Map<String, String> confMap = Utilities.getConf(conf, mysqlConfKeyPrefix);
    for (String key : confMap.keySet()) {
      String value = confMap.get(key);
      // mysql.server.conf.mysqlKey0.mysqlKey1
      key = key.substring(mysqlConfKeyPrefix.length());
      String[] mysqlKey = key.split("\\.", 2);
      if (mysqlKey.length < 2) continue;
      if (mysqlConfMap.get(mysqlKey[0]) == null) mysqlConfMap.put(mysqlKey[0], new HashMap<String, String>());
      mysqlConfMap.get(mysqlKey[0]).put(mysqlKey[1], value);
    }

    // generate mysql configuration string
    StringBuilder mysqlConfStringBuilder = new StringBuilder(1024);
    for (String mysqlConfPartKey : mysqlConfMap.keySet()) {
      mysqlConfStringBuilder.append("[").append(mysqlConfPartKey).append("]\n");
      Map<String, String> mysqlConfPartMap = mysqlConfMap.get(mysqlConfPartKey);
      List<String> mysqlConfKeys = new ArrayList<String>(mysqlConfPartMap.keySet());
      Collections.sort(mysqlConfKeys);
      for (String mysqlConfKey : mysqlConfKeys) {
        mysqlConfStringBuilder.append(mysqlConfKey);
        String value = mysqlConfPartMap.get(mysqlConfKey);
        if (value != null && !value.isEmpty()) mysqlConfStringBuilder.append("=").append(value);
        mysqlConfStringBuilder.append("\n");
      }
    }

    // save mysql configuration to .cnf file
    RandomAccessFile mysqlConfFile =
        new RandomAccessFile(Utilities.getNormalPath(conf.get("mysql.server.data.path", ".")) + "/my.cnf", "rwd");
    mysqlConfFile.setLength(0);
    mysqlConfFile.write(mysqlConfStringBuilder.toString().getBytes());
    mysqlConfFile.close();
  }

  public static String getMysqlConf(Configuration conf, String mysqlConfKey) {
    return getMysqlConf(conf, mysqlConfKey, null);
  }

  public static String getMysqlConf(Configuration conf, String mysqlConfKey, Object defaultValue) {
    if (defaultValue == null) return conf.get(mysqlConfKeyPrefix + mysqlConfKey);
    else return conf.get(mysqlConfKeyPrefix + mysqlConfKey, defaultValue.toString());
  }

  public static void setMysqlConf(Configuration conf, String mysqlConfKey, String value) {
    conf.set(mysqlConfKeyPrefix + mysqlConfKey, value);
  }

  void setMysqlDefaultConf(Configuration conf) throws IOException {
    setMysqlBinPermission(conf);
    String dataPath = Utilities.getNormalPath(conf.get("mysql.server.data.path", "."));
    String defaultMysqlBaseDir =
        new File(Utilities.runCommand("which mysqld", 0, conf.get("mysql.server.bin.path"), null)).getParentFile()
            .getParent();
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.user", Utilities.getCurrentUser());
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.bind-address", "0.0.0.0");
    String mysqlServerPort = Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.port", "40001");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.basedir", defaultMysqlBaseDir, true);
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.datadir", dataPath, true);
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.tmpdir", dataPath, true);
    String socketFilePath = dataPath + "/mysqld.sock";
    if (socketFilePath.length() > 107) throw new IOException("socket file path is too long (>107): " + socketFilePath);
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.socket", socketFilePath, true);
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.log_error", dataPath + "/error.log", true);
    if (conf.get("mysql.server.conf.mysqld.external_locking") != null
        && conf.get("mysql.server.conf.mysqld.skip_external_locking") == null) {
      Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.skip_external_locking", "");
    } else Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.skip_external_locking", "");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.open_files_limit", "65535");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.max_connections", "1000");
    Utilities
        .setConfDefaultValue(
            conf,
            "mysql.server.conf.mysqld.plugin-load",
            "innodb=ha_innodb_plugin.so;innodb_trx=ha_innodb_plugin.so;innodb_locks=ha_innodb_plugin.so;innodb_lock_waits=ha_innodb_plugin.so;innodb_cmp=ha_innodb_plugin.so;innodb_cmp_reset=ha_innodb_plugin.so;innodb_cmpmem=ha_innodb_plugin.so;innodb_cmpmem_reset=ha_innodb_plugin.so;handlersocket.so;tdh_socket=tdhsocket.so");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.ignore_builtin_innodb", "");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_file_per_table", "TRUE");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_buffer_pool_size", "8M");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_open_files", "65535");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_read_io_threads", "8");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_write_io_threads", "8");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_flush_method", "O_DIRECT");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_flush_log_at_trx_commit", "1");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_log_file_size", "256M");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_log_buffer_size", "32M");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.innodb_log_files_in_group", "2");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.loose_handlersocket_port", Integer
        .valueOf(mysqlServerPort) + 1);
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.loose_handlersocket_port_wr", Integer
        .valueOf(mysqlServerPort) + 2);
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.loose_handlersocket_threads", "4");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.loose_handlersocket_threads_wr", "4");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.tdh_socket_listen_port", Integer
        .valueOf(mysqlServerPort) + 3);
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.tdh_socket_thread_num", "4");
    Utilities.setConfDefaultValue(conf, "mysql.server.conf.mysqld.tdh_socket_write_thread_num", "4");
  }

  void setMysqlBinPermission(Configuration conf) throws IOException {
    String mysqlBinPath = conf.get("mysql.server.bin.path");
    if (mysqlBinPath == null || mysqlBinPath.isEmpty()) return;
    Utilities.runCommand("chmod -R 755 " + conf.get("mysql.server.bin.path"), null, null, null);
  }

  /**
   * @return old mysql server pid
   */
  public String stopServer(Configuration conf) throws IOException {
    // stop mysql server
    setMysqlDefaultConf(conf);
    String mysqlServerPid = getServerPid(conf);
    if (mysqlServerPid.isEmpty()) return mysqlServerPid;
    String commandForStopMysqld = "kill -9  " + mysqlServerPid;
    Utilities.logInfo(logger, "mysql server is stopping, command=", commandForStopMysqld);
    for (int i = 0; i < 10; ++i) {
      Utilities.runCommand(commandForStopMysqld, null, null, null);
      if (getServerPid(conf).isEmpty()) {
        Utilities.logInfo(logger, "mysql server has stopped");
        return mysqlServerPid;
      }
      Utilities.sleepAndProcessInterruptedException(1000, logger);
    }
    throw new IOException("fail to stop mysql with command=" + commandForStopMysqld);
  }

  String getServerPid(Configuration conf) throws IOException {
    String mysqlConfPath = Utilities.getNormalPath(conf.get("mysql.server.data.path", ".")) + "/my.cnf";
    String commandForGetMysqldPid =
        "ps -ef|grep -v grep|grep \"mysqld" + " --defaults-file=" + mysqlConfPath + "\"|awk '{print $2}'";
    return Utilities.runCommand(commandForGetMysqldPid, null, null, null);
  }

  List<String> getServerListenAddresses(Configuration conf) throws IOException {
    return Utilities.getListenAddressList(new String[] { getServerPid(conf) }, null, new String[] { "4", "7" });
  }
}
