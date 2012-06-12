/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.distributed;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.WriterAppender;
import org.apache.log4j.helpers.QuietWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedManager.ServerStatuses;
import com.taobao.adfs.distributed.editlogger.DistributedEditLogger;
import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.distributed.rpc.RPC;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.distributed.rpc.Server;
import com.taobao.adfs.util.IpAddress;
import com.taobao.adfs.util.ReentrantReadWriteLockExtension;
import com.taobao.adfs.util.Utilities;
import com.taobao.adfs.util.Utilities.LongTime;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 * @created 2011-05-17
 */
public class DistributedServer implements DistributedInvocable {
  public static final Logger logger = LoggerFactory.getLogger(DistributedServer.class);
  public static DistributedMetrics distributedMetrics = null;
  String serverName = null;
  Configuration conf = null;
  DistributedData data = null;
  Server rpcServer;
  DistributedManager distributedManager = null;
  volatile ServerType serverType = ServerType.NEED_RESTORE;
  ServerStatuses serverStatuses = new ServerStatuses();
  DistributedEditLogger editLogger = null;
  Object statusLock = new Object();
  /**
   * getData invocation will block format invocation immediately,
   * and block other data invocations in the end of getData invocation
   */
  ReentrantReadWriteLockExtension getDataLocker = new ReentrantReadWriteLockExtension();

  public DistributedServer(DistributedData data) throws IOException {
    this.data = data;
    this.data.setDataLocker(getDataLocker);
    this.conf = (data.conf == null) ? new Configuration(false) : data.conf;
    serverType = ServerType.ONLINE;
    initialize();
  }

  boolean initialize() throws IOException {
    invocationToGetStatusForLog = new Invocation(this, "getStatus");
    serverName = getServerName(conf);
    threadLocalInvocation.set(new Invocation());
    threadLocalInvocation.get().setCallerAddress(IpAddress.getAddress(Utilities.getHost(serverName)));
    threadLocalInvocation.get().setCallerProcessId(Utilities.getPid());
    threadLocalInvocation.get().setCallerThreadId(Thread.currentThread().getId());
    threadLocalInvocation.get().setCallerThreadName(Thread.currentThread().getName());
    threadLocalInvocation.get().setCallerSequenceNumber(0);
    addShutdownHook();
    createLoggerThread();
    createMetrics();
    createManager();
    createEditLogger();
    createRpcServer();
    createLeaseThread();
    createCheckThread();
    log(Level.INFO, " starts up");
    return true;
  }

  static String getServerName(Configuration conf) throws IOException {
    String serverName = StringUtils.deleteWhitespace(conf.get("distributed.server.name"));
    if (serverName == null || serverName.split(":").length != 2) {
      String host = conf.get("distributed.server.host", "").trim();
      if (host.isEmpty()) {
        List<InetAddress> inetAddressList = Utilities.getInetAddressList();
        host = inetAddressList.isEmpty() ? "localhost" : inetAddressList.get(0).getHostAddress();
      }
      serverName = host + ":50000";
    }
    String[] serverNameHostAndPort = serverName.split(":");
    serverNameHostAndPort[1] = conf.get("distributed.server.port", serverNameHostAndPort[1]);
    serverName = serverNameHostAndPort[0] + ":" + serverNameHostAndPort[1];
    conf.set("distributed.server.name", serverName);
    return serverName;
  }

  public static void configLogger(Configuration conf) throws IOException {
    String loggerConfKeyPrefix = "distributed.logger.conf.";
    Utilities.setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.rootLogger", "INFO,DistributedServer");
    Utilities.setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.threshhold", "ALL");
    Utilities.setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.appender.console",
        "org.apache.log4j.ConsoleAppender");
    Utilities.setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.appender.console.target", "System.err");
    Utilities.setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.appender.console.layout",
        "org.apache.log4j.PatternLayout");
    Utilities.setConfDefaultValue(conf, loggerConfKeyPrefix + "log4j.appender.console.layout.ConversionPattern",
        "%d{yy-MM-dd HH:mm:ss,SSS} %p %c{2}: %m%n");
    String appenderName = loggerConfKeyPrefix + "log4j.appender." + DistributedServer.class.getSimpleName();
    Utilities.setConfDefaultValue(conf, appenderName, "org.apache.log4j.DailyRollingFileAppender");
    Utilities.setConfDefaultValue(conf, appenderName + ".File", "logs/distributed-server-"
        + conf.get("distributed.server.name", "defaultServerName").replace(':', '-') + ".log");
    Utilities.setConfDefaultValue(conf, appenderName + ".layout", "org.apache.log4j.PatternLayout");
    Utilities.setConfDefaultValue(conf, appenderName + ".layout.ConversionPattern",
        "%d{yy-MM-dd HH:mm:ss,SSS} %p %c{2}: %m%n");
    Utilities.setConfDefaultValue(conf, appenderName + ".BufferedIO", "true");
    Utilities.setConfDefaultValue(conf, appenderName + ".BufferSize", "1048576");

    Utilities.configureLog4j(conf, loggerConfKeyPrefix, Level.INFO);
    Utilities.setLoggerLevel(conf, logger);
  }

  void createMetrics() throws IOException {
    if (!conf.getBoolean("distributed.metrics.enable", true)) return;
    distributedMetrics = new DistributedMetrics(conf);
    distributedMetrics.open("server");
  }

  void createManager() throws IOException {
    Utilities.setConfDefaultValue(conf, "distributed.manager.name", data.getClass().getSimpleName());
    distributedManager = new DistributedManager(conf);
    Utilities.logInfo(logger, "create server manager with address=", distributedManager.getAddress());
    distributedManager.unregister(serverName);
  }

  public static String getDataPath(Configuration conf) throws IOException {
    String dataPath = conf.get("distributed.data.path", getServerName(conf));
    if (dataPath.contains(":")) dataPath = dataPath.replaceAll(":", "-");
    conf.set("distributed.data.path", dataPath);
    return dataPath;
  }

  void createData() throws IOException {
    String dataPath = getDataPath(conf);
    try {
      String dataClassName = conf.get("distributed.data.class.name");
      if (dataClassName == null) throw new IOException("distributed.data.class.name is not specified");
      Class<?> dataClass = Class.forName(dataClassName);
      Constructor<?> dataConstructor =
          dataClass.getDeclaredConstructor(Configuration.class, ReentrantReadWriteLockExtension.class);
      dataConstructor.setAccessible(true);
      data = (DistributedData) dataConstructor.newInstance(conf, getDataLocker);
      if (conf.getBoolean("distributed.data.initialize.result", false)) serverType = ServerType.ONLINE;
      else serverType = ServerType.NEED_RESTORE;
      log(Level.INFO, " create data with path=", dataPath);
    } catch (Throwable t) {
      Utilities.logError(logger, " fail to create data with path=", dataPath, t);
      throw new IOException(t);
    }
  }

  void createEditLogger() throws IOException {
    editLogger = DistributedEditLogger.getDistributedEditLogger(conf, data);
  }

  void createRpcServer() throws IOException {
    InetSocketAddress socAddr = NetUtils.createSocketAddr(serverName);
    int rpcHandlerNumber = conf.getInt("distributed.server.handler.number", 100);
    for (int i = 0; i < 10; ++i) {
      try {
        rpcServer = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(), rpcHandlerNumber, false, conf);
        break;
      } catch (IOException e) {
        if (i == 9 || !Utilities.getFirstCause(e).getClass().equals(BindException.class)) throw e;
        Utilities.sleepAndProcessInterruptedException(1000, logger);
      }
    }
    try {
      rpcServer.start();
      log(Level.INFO, " create rpc server with address=", serverName);
    } catch (Throwable t) {
      log(Level.ERROR, " fail to create rpc server with address=", serverName, t);
      rpcServer = null;
      throw new IOException(t);
    }
  }

  public Server getRpcServer() {
    return rpcServer;
  }

  public ServerType getServerType() {
    return serverType;
  }

  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return DistributedInvocable.versionID;
  }

  public void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(serverName) {
      public void run() {
        Utilities.logInfo(logger, "jvm for distributed server ", getName(), " has been shut down");
        logFlush();
      }
    });
  }

  DistributedInvocable createServerProxy(String serverName) throws IOException {
    if (serverName == null) throw new IOException("fail to create server proxy for serverName=" + null);
    DistributedInvocable distributedProxy =
        (DistributedInvocable) RPC.getProxy(DistributedInvocable.class, DistributedInvocable.versionID, NetUtils
            .createSocketAddr(serverName), conf);
    return distributedProxy;
  }

  void createLoggerThread() {
    class DistributedLoggerThread extends Thread {
      DistributedLoggerThread() {
        setDaemon(true);
        setName(getClass().getSimpleName() + "@" + getName());
      }

      public void run() {
        while (true) {
          try {
            if (serverType.equals(ServerType.STOP)) break;
            logFlush();
            long loggerFlushInterval = conf.getLong("distributed.logger.flush.interval", 1000);
            Utilities.sleepAndProcessInterruptedException(loggerFlushInterval, logger);
          } catch (Throwable t) {
            Utilities.logWarn(logger, t);
          }
        }
        Utilities.logInfo(logger, getName(), " has stopped");
      }
    };
    new DistributedLoggerThread().start();
  }

  void createLeaseThread() {
    class DistributedLeaseThread extends Thread {
      DistributedLeaseThread() {
        setDaemon(true);
        setName(getClass().getSimpleName() + "@" + getName());
      }

      public void run() {
        while (true) {
          try {
            updateServers();
            if (serverType.equals(ServerType.STOP)) {
              DistributedServer.this.stop();
              break;
            }
            long leaseInterval = conf.getLong("distributed.server.lease.interval", 1000);
            Utilities.sleepAndProcessInterruptedException(leaseInterval, logger);
          } catch (Throwable t) {
            Utilities.logWarn(logger, " fail to update distributed lease ", t);
          }
        }
        Utilities.logInfo(logger, getName(), " has stopped");
      }
    };
    new DistributedLeaseThread().start();
  }

  void createCheckThread() {
    class DistributedCheckThread extends Thread {
      DistributedCheckThread() {
        setDaemon(true);
        setName(getClass().getSimpleName() + "@" + getName());
      }

      public void run() {
        while (true) {
          try {
            long checkInterval = conf.getLong("distributed.server.check.interval", 1000);
            Utilities.sleepAndProcessInterruptedException(checkInterval, logger);
            if (editLogger.failToApply.get()) {
              register(serverName, data.getDataVersion(), ServerType.NEED_RESTORE);
              editLogger.open();
            }
            if (serverType.equals(ServerType.STOP)) break;
            if (serverType.not(ServerType.NEED_RESTORE, ServerType.UNDER_RESTORE) && !data.isValid()) {
              if (!data.isValid()) {
                log(Level.ERROR, " check thread detects data is invalid and register to be ", ServerType.NEED_RESTORE);
                register(serverName, data.getDataVersion(), ServerType.NEED_RESTORE);
              }
            }
            log(Level.TRACE, " Servers=", serverStatuses.toStringWithNameVersionTypeApplied());
            // set metrics for server type
            for (ServerType serverType : ServerType.values()) {
              DistributedMetrics.intValueaSet("type." + serverType.toString(), serverType
                  .equals(DistributedServer.this.serverType) ? 1 : 0);
            }
            restoreFromMasterServer();
          } catch (Throwable t) {
            Utilities.logWarn(logger, " fail to check distributed server ", t);
          }
        }
        Utilities.logInfo(logger, " ", getName(), " has stopped");
      }
    };
    new DistributedCheckThread().start();
  }

  synchronized boolean register(String serverName, Long dataVersion, ServerType serverType) {
    try {
      ServerType oldServerType = this.serverType;
      boolean changeSelfVersionOnly =
          this.serverName.equals(serverName) && this.serverType.equals(serverType)
              && serverStatuses.isTypeApplied(serverName);
      log(changeSelfVersionOnly ? Level.DEBUG : Level.INFO, " register ", serverName, "|", dataVersion, "|", serverType);
      boolean changeSelfType = this.serverName.equals(serverName) && !this.serverType.equals(serverType);
      boolean becomeMaster = changeSelfType && ServerType.MASTER.is(serverType);
      boolean leaveMaster = changeSelfType && !ServerType.MASTER.is(serverType);

      if (becomeMaster && data != null && !data.becomeMasterPre(oldServerType)) {
        String errorString = "fail to become master-pre for data";
        log(Level.ERROR, errorString);
        throw new IOException(errorString);
      }
      if (leaveMaster && data != null && !data.leaveMasterPre(serverType)) {
        String errorString = "fail to leave master-pre for data";
        log(Level.ERROR, errorString);
        throw new IOException(errorString);
      }

      if (changeSelfType) this.serverType = serverType;
      distributedManager.register(serverName, dataVersion, serverType, this.serverName);

      if (becomeMaster && data != null && !data.becomeMasterPost(oldServerType)) {
        String errorString = "fail to become master-Post for data";
        log(Level.ERROR, errorString);
        throw new IOException(errorString);
      }
      if (leaveMaster && data != null && !data.leaveMasterPost(serverType)) {
        String errorString = "fail to leave master-Post for data";
        log(Level.ERROR, errorString);
        throw new IOException(errorString);
      }

      // wait until new type is applied or known by clients(client lease is timeout)
      if (!this.serverName.equals(serverName)) {
        for (int i = 0; i * 100 < (getLeaseTimeout() < 1000 ? 1000 : getLeaseTimeout()) * 2; ++i) {
          ServerStatus serverStatus = distributedManager.getServers(null).get(serverName);
          if (serverStatus != null && serverStatus.isTypeApplied()) return true;
          Utilities.sleepAndProcessInterruptedException(100, logger);
        }
        log(Level.WARN, " wait timeout when register ", serverName, "|", dataVersion, "|", serverType);
        return false;
      } else {
        return true;
      }
    } catch (Throwable t) {
      log(Level.ERROR, " failed to register ", serverName, "|", dataVersion, "|", serverType, ", jvm will exit.", t);
      logFlush();
      System.exit(1);
      while (true);
    }
  }

  boolean unregisterWhenExpired(ServerStatus serverStatus) {
    try {
      if (serverStatus == null) return false;
      if (System.currentTimeMillis() - serverStatus.createTime < distributedManager.getSessionTimeout()) return false;
      unregister(serverStatus.name);
      return true;
    } catch (Throwable t) {
      log(Level.ERROR, " fail to getSessionTimeout, jvm will exit.", t);
      logFlush();
      System.exit(1);
      while (true);
    }
  }

  synchronized void unregister(String serverName) {
    try {
      boolean leaveMaster = this.serverName.equals(serverName) && ServerType.MASTER.is(this.serverType);
      if (leaveMaster && data != null && !data.leaveMasterPre(null)) {
        String errorString = "fail to leave master-pre for data";
        log(Level.ERROR, errorString);
        throw new IOException(errorString);
      }
      distributedManager.unregister(serverName);
      if (leaveMaster && data != null && !data.leaveMasterPost(null)) {
        String errorString = "fail to leave master-post for data";
        log(Level.ERROR, errorString);
        throw new IOException(errorString);
      }
      log(Level.INFO, " unregister ", serverName);
      if (distributedManager.getServers(null).get(serverName) != null)
        throw new IOException("found " + serverName + " again");
    } catch (Throwable t) {
      log(Level.ERROR, " fail to unregister ", serverName, ", jvm will exit.", t);
      logFlush();
      System.exit(1);
      while (true);
    }
  }

  Invocation invocationToGetStatusForLog = null;

  Object[] getStatus() {
    Object[] objects = new Object[13];
    objects[0] = distributedManager.getName();
    objects[1] = "@";
    objects[2] = distributedManager.getAddress();
    objects[3] = "|";
    objects[4] = serverName;
    objects[5] = "|";
    objects[6] = data.getDataVersion();
    objects[7] = "|";
    objects[8] = serverType;
    objects[9] = "|threadId=";
    objects[10] = Thread.currentThread().getId();
    objects[11] = "|threadName=";
    objects[12] = Thread.currentThread().getName();
    return objects;
  }

  void log(Level level, Object... objects) {
    Utilities.log(logger, level, invocationToGetStatusForLog, objects);
  }

  void logFlush() {
    try {
      List<org.apache.log4j.Logger> loggers = Utilities.getLoggers();
      HashSet<Appender> appenderSet = new HashSet<Appender>();
      for (org.apache.log4j.Logger logger : loggers) {
        Enumeration<?> appenders = logger.getAllAppenders();
        while (appenders.hasMoreElements()) {
          Appender appender = (Appender) appenders.nextElement();
          appenderSet.add(appender);
        }
      }
      for (Appender appender : appenderSet) {
        if (WriterAppender.class.isAssignableFrom(appender.getClass())) {
          QuietWriter quietWriter = (QuietWriter) Utilities.getFieldValue(appender, "qw");
          if (quietWriter != null) quietWriter.flush();
        }
      }
    } catch (Throwable t) {
      log(Level.ERROR, " fail to flush log4j mannuly", t);
    }
  }

  synchronized public ServerStatuses updateServers() {
    // get serverStatuses and make sure this server has been registered, otherwise register it and return directly
    ServerStatuses newServerStatuses = distributedManager.getServers(serverName);

    ServerStatus newServerStatusOfThisServer = newServerStatuses.get(serverName);
    if (newServerStatusOfThisServer == null) {
      if (serverType.is(ServerType.STOP)) unregister(serverName);
      else if (serverType.is(ServerType.STANDBY)) register(serverName, data.getDataVersion(), ServerType.STANDBY);
      else if (serverType.is(ServerType.ONLINE)) register(serverName, data.getDataVersion(), ServerType.ONLINE);
      else if (serverType.is(ServerType.MASTER)) register(serverName, data.getDataVersion(), ServerType.ONLINE);
      else if (serverType.is(ServerType.SLAVE)) register(serverName, data.getDataVersion(), ServerType.ONLINE);
      else register(serverName, data.getDataVersion(), ServerType.NEED_RESTORE);
      return serverStatuses;
    }

    if (newServerStatusOfThisServer.type.equals(ServerType.MASTER)) {
      for (ServerStatus newServerStatus : newServerStatuses.getNot(false, ServerType.MASTER)) {
        ServerStatus oldServerStatus = serverStatuses.get(newServerStatus.name);
        if (oldServerStatus != null && !oldServerStatus.type.equals(newServerStatus.type))
          log(Level.INFO, " serverType changed: ", newServerStatus, " <- ", oldServerStatus);
        // master server needs to register new type for other servers whose type is changed by manager
        if (newServerStatus.isTypeRegistered()) {
          // if you have many servers, asynchronous register logic will perform better
          try {
            if (!unregisterWhenExpired(newServerStatus)) {
              boolean result = register(newServerStatus.name, newServerStatus.version, newServerStatus.type);
              if (result) newServerStatus.setterName = newServerStatus.name;
              else {
                log(Level.WARN, " fail to apply ", serverName, "|", newServerStatus.version, "|", serverType);
                newServerStatus.setterName = serverName;
              }
            }
          } catch (Throwable t) {
            log(Level.WARN, "fail to ", t);
          }
        }
        // master server needs to create RPC to other servers whose type is not be STOP
        if (newServerStatus.type.equals(ServerType.STOP)) continue;
        ServerStatus oldServerStatues = (serverStatuses == null) ? null : serverStatuses.get(newServerStatus.name);
        if (oldServerStatues != null) newServerStatus.proxy = oldServerStatues.proxy;
        try {
          if (newServerStatus.proxy == null) {
            newServerStatus.proxy = createServerProxy(newServerStatus.name);
            log(Level.INFO, " create rpc to ", serverName);
          }
        } catch (Throwable t) {
          log(Level.ERROR, " fail to create RPC to ", newServerStatus.toString(), t);
          boolean result = register(newServerStatus.name, newServerStatus.version, ServerType.NEED_RESTORE);
          if (result) newServerStatus.setterName = newServerStatus.name;
          else newServerStatus.setterName = serverName;
        }
      }
    }

    // apply new status and notify manager that new type has been applied if new type is set by others
    if (!newServerStatusOfThisServer.type.is(serverType, ServerType.MASTER))
      log(Level.INFO, " change server type to be ", newServerStatusOfThisServer.type, " by ",
          newServerStatusOfThisServer.setterName);
    synchronized (statusLock) {
      serverStatuses = newServerStatuses;
      if (!newServerStatusOfThisServer.isTypeApplied()
          || !newServerStatusOfThisServer.version.equals(data.getDataVersion())) {
        newServerStatusOfThisServer.setterName = serverName;
        newServerStatusOfThisServer.version = data.getDataVersion();
        register(serverName, data.getDataVersion(), newServerStatusOfThisServer.type);
      }
    }
    conf.set("distributed.server.type", serverType.toString());

    return serverStatuses;
  }

  public DistributedData getData(DistributedData oldData) throws IOException {
    String remoteServerName = (String) oldData.getElementToTransfer("distributed.server.name");
    log(Level.INFO, " receive get data request from ", remoteServerName, "|Version=", oldData.getDataVersion());
    if (serverName.equals(remoteServerName)) {
      log(Level.WARN, " request server is self=", remoteServerName, "|Version=", oldData.getDataVersion());
      throw new IOException("request server is self=" + remoteServerName + "|Version=" + oldData.getDataVersion());
    }
    if (!ServerType.MASTER.equals(serverType)) {
      log(Level.WARN, " server is not master, refuse to get data request from ", remoteServerName, "|Version=", oldData
          .getDataVersion());
      throw new IOException("server is not a master, refuse to  get data request from " + remoteServerName
          + "|Version=" + oldData.getDataVersion());
    }

    // wait all edit logs have been applied for new master and check new server type after waiting
    editLogger.waitUntilWorkSizeIsEmpty();
    if (!ServerType.MASTER.equals(serverType)) {
      log(Level.WARN, " server is not master, refuse to get data request from ", remoteServerName, "|Version=", oldData
          .getDataVersion());
      throw new IOException("server is not a master, refuse to get data request from " + remoteServerName + "|Version="
          + oldData.getDataVersion());
    }
    try {
      DistributedData newData = null;
      if (!oldData.getIsIncrementRestoreEnabled()) {
        try {
          newData = data.getData(oldData, getDataLocker.writeLock());
          newData.setDataVersion(data.getDataVersion());
          log(Level.INFO, " get all data with version=", newData.getDataVersion(), " for ", remoteServerName);
          boolean isTypeApplied = register(remoteServerName, -1L, ServerType.UNDER_RESTORE);
          if (!isTypeApplied)
            throw new IOException("fail to apply new type to be " + ServerType.UNDER_RESTORE + " for "
                + remoteServerName);
          log(Level.INFO, " register and apply new type to be ", ServerType.UNDER_RESTORE, " for ", remoteServerName);
          for (int i = 0; i < 100; ++i) {
            ServerStatus serverStatus = serverStatuses.get(remoteServerName);
            if (serverStatus != null && serverStatus.type.is(ServerType.UNDER_RESTORE)) break;
            if (i == 100 - 1)
              throw new IOException("fail to get new type " + ServerType.UNDER_RESTORE + " for " + remoteServerName);
            Utilities.sleepAndProcessInterruptedException(100, logger);
          }
          log(Level.INFO, " get new type ", ServerType.UNDER_RESTORE, " for ", remoteServerName);
          return newData;
        } catch (Throwable t) {
          log(Level.ERROR, " fail to get all data for ", remoteServerName, ", register new type to be ",
              ServerType.NEED_RESTORE, ", exception=", t);
          register(remoteServerName, -1L, ServerType.NEED_RESTORE);
          throw new IOException(t);
        }
      } else {
        try {
          long versionFrom = (Long) oldData.getElementToTransfer("distributed.data.restore.increment.version.from");
          long versionTo = (Long) oldData.getElementToTransfer("distributed.data.restore.increment.version.to");
          if (versionFrom < versionTo) {
            newData = data.getData(oldData, getDataLocker.writeLock());
            log(Level.INFO, " get increment data from V", versionFrom, " to V", versionTo, " for ", remoteServerName);
            return newData;
          } else if (versionFrom == versionTo) {
            getDataLocker.writeLock().lock();
            log(Level.INFO, " block write request for increment data restore");
            if (data.getDataVersion() > versionTo) {
              log(Level.INFO, " get increment data from V", versionFrom, " to V", versionTo, " for ", remoteServerName);
              return newData = data.getData(oldData, getDataLocker.writeLock());
            }
            boolean isTypeApplied = register(remoteServerName, data.getDataVersion(), ServerType.SLAVE);
            if (!isTypeApplied) throw new IOException("fail to apply new registered type to be " + ServerType.SLAVE);
            return null;
          } else throw new IOException("versionFrom=" + versionFrom + " is large than versionTo=" + versionTo);
        } catch (Throwable t) {
          log(Level.ERROR, "fail to get increment data for ", remoteServerName, ", register new type to be ",
              ServerType.NEED_RESTORE, t);
          register(remoteServerName, -1L, ServerType.NEED_RESTORE);
          throw new IOException(t);
        }
      }
    } finally {
      if (getDataLocker.isWriteLockedByCurrentThread()) {
        getDataLocker.writeLock().unlock();
        log(Level.INFO, " resume write request for data restore");
      }
    }
  }

  DistributedData getDataFromMaster(ServerStatus master) throws IOException {
    Long versionFrom = (Long) data.getElementToTransfer("distributed.data.restore.increment.version.from");
    Long versionTo = (Long) data.getElementToTransfer("distributed.data.restore.increment.version.to");
    if (versionFrom != null && versionTo != null) log(Level.INFO, " start to get data of master=", master.name,
        " from V", versionFrom, " to V", versionTo);
    else log(Level.INFO, " start to get data of master=", master.name);
    DistributedInvocable masterProxy =
        (DistributedInvocable) RPC.getProxy(DistributedInvocable.class, DistributedInvocable.versionID, NetUtils
            .createSocketAddr(master.name), conf);
    Method methodForGetData = Invocation.getMethod(getClass(), "getData", DistributedData.class);
    Invocation invocationForGetData = new Invocation(methodForGetData, data);
    DistributedData masterData = (DistributedData) masterProxy.invoke(invocationForGetData);
    if (masterData == null) log(Level.INFO, " succeed in getting null data of master=", master.name);
    else {
      versionFrom = (Long) masterData.getElementToTransfer("distributed.data.restore.increment.version.from");
      versionTo = (Long) masterData.getElementToTransfer("distributed.data.restore.increment.version.to");
      if (versionFrom != null && versionTo != null) log(Level.INFO, " succeed in getting increment data of master=",
          master.name, " from V", versionFrom, " to V", versionTo);
      else log(Level.INFO, " succeed in getting all data of master=", master.name, ", new data version=", masterData
          .getDataVersion());
    }
    return masterData;
  }

  void restoreAllFromMasterServer(ServerStatus master) {
    try {
      log(Level.INFO, " start to restore all from ", master.name);
      data.setIsIncrementRestoreEnabled(false);
      data.close();
      data.backup();
      DistributedData dataFromMaster = getDataFromMaster(master);
      log(Level.INFO, " start to restore all data from master=", master.name);
      data.setData(dataFromMaster);
      log(Level.INFO, " succeed in restoring all data from master=", master.name);
      data.open();
      // some fail operation will cause current version large than max data version
      data.setDataVersion(dataFromMaster.getDataVersion());
      data.setIsInIncrementRestoreStage(false, false);
      register(serverName, data.getDataVersion(), ServerType.SLAVE);
      log(Level.INFO, " succeed in restoring all from ", master.name);
    } catch (Throwable t) {
      log(Level.ERROR, " fail to restore all from ", master.name, t);
      register(serverName, data.getDataVersion(), ServerType.NEED_RESTORE);
      closeData();
      editLogger.open();
    }
  }

  void restoreIncrementFromMasterServerInternal(ServerStatus master, boolean firstCycle) throws IOException {
    // check version gap
    long incrementVersionGapMax = conf.getLong("distributed.data.restore.increment.version.gap.max", -1);
    if (incrementVersionGapMax > Integer.MAX_VALUE) {
      incrementVersionGapMax = Integer.MAX_VALUE;
      log(Level.WARN,
          " distributed.data.restore.increment.version.gap.max is too large, set it to be Integer.MAX_VALUE");
    }
    if (incrementVersionGapMax < 0) throw new IOException("increment restore is disabled");
    long dataVersionOfThisServer = data.getDataVersion();
    if (dataVersionOfThisServer < 0) throw new IOException("fail to get this server's data version");
    long dataVersionOfMasterServer = new DistributedMonitor(conf, master.name).getDataVersion();
    long versionGap = Math.abs(dataVersionOfMasterServer - dataVersionOfThisServer);
    if (versionGap > incrementVersionGapMax) throw new IOException("too large version gap for increment restore");

    // check version from and version to
    long incrementVersionFrom = dataVersionOfThisServer;
    long incrementVersionTo = dataVersionOfMasterServer;
    if (incrementVersionFrom > incrementVersionTo) {
      incrementVersionFrom = dataVersionOfMasterServer;
      incrementVersionTo = dataVersionOfThisServer;
    }
    if (firstCycle)
      incrementVersionFrom -= conf.getLong("distributed.data.restore.increment.version.revert", Long.MAX_VALUE);
    if (incrementVersionFrom < 0) incrementVersionFrom = 0;

    // get and set increment data
    data.putElementToTransfer("distributed.data.restore.increment.version.from", incrementVersionFrom);
    data.putElementToTransfer("distributed.data.restore.increment.version.to", incrementVersionTo);
    // get distributed.data.restore.increment.by.version and save it in this.data
    data.getData(data, getDataLocker.writeLock());
    DistributedData dataFromMaster = getDataFromMaster(master);
    if (dataFromMaster == null) return;
    log(Level.INFO, " start to restore increment data from master=", master.name, " with version from ",
        incrementVersionFrom, " to ", incrementVersionTo);
    data.setData(dataFromMaster);
    log(Level.INFO, " succeed in restoring increment data from master=", master.name, " with version from ",
        incrementVersionFrom, " to ", incrementVersionTo);
    data.setDataVersion(dataVersionOfMasterServer);
  }

  boolean restoreIncrementFromMasterServer(ServerStatus master) {
    try {
      log(Level.INFO, " start to restore increment from ", master.name);
      data.setIsIncrementRestoreEnabled(true);
      data.open();
      if (data.getIsInIncrementRestoreStage(false)) throw new IOException("last increment restore is failed");
      data.setIsInIncrementRestoreStage(true, false);
      register(serverName, data.getDataVersion(), ServerType.UNDER_RESTORE);
      boolean firstCycle = true;
      // do increment restore until status is not UNDER_RESTORE
      while (serverType.equals(ServerType.UNDER_RESTORE)) {
        restoreIncrementFromMasterServerInternal(master, firstCycle);
        firstCycle = false;
      }
      data.setIsInIncrementRestoreStage(false, false);
      log(Level.INFO, " succeed in restoring increment from ", master.name);
      return true;
    } catch (Throwable t) {
      log(Level.WARN, " fail to restore increment from ", master.name, t);
      register(serverName, data.getDataVersion(), ServerType.NEED_RESTORE);
      closeData();
      return false;
    } finally {
      try {
        data.setIsInIncrementRestoreStage(false, true);
      } catch (Throwable t) {
        log(Level.WARN, " fail to set distributed.data.restore.increment.stage to be false");
      }
    }
  }

  void restoreFromMasterServer() {
    if (!serverType.equals(ServerType.NEED_RESTORE)) return;
    editLogger.clear();
    editLogger.pauseApply();
    try {
      ServerStatus master = getMasterUntilNotNull();
      if (master == null || serverName.equals(master.name)) return;
      boolean isIncrementRestoreEnabled = data.getIsIncrementRestoreEnabled();
      if (isIncrementRestoreEnabled) {
        if (!restoreIncrementFromMasterServer(master)) restoreAllFromMasterServer(master);
      } else restoreAllFromMasterServer(master);
      data.setIsIncrementRestoreEnabled(isIncrementRestoreEnabled);
    } finally {
      editLogger.resumeApply();
    }
  }

  ServerStatus getMasterUntilNotNull() {
    // wait until master is elected
    ServerStatus master = serverStatuses.getMaster(true);
    while (master == null) {
      log(Level.INFO, " wait master election");
      Utilities.sleepAndProcessInterruptedException(1000, logger);
      master = updateServers().getMaster(true);
    }
    return master;
  }

  public void stop() throws IOException {
    log(Level.INFO, " is stopping ...");
    editLogger.close(false);
    closeData();
    rpcServer.stop();
    unregister(serverName);
    if (distributedMetrics != null) distributedMetrics.shutdown();
    if (distributedManager != null) distributedManager.close();
    log(Level.INFO, " has been stopped");
    serverType = ServerType.STOP;
  }

  public void waitUntilStop() throws IOException {
    try {
      while (true) {
        if (serverType.equals(ServerType.STOP)) {
          rpcServer.join();
          break;
        }
        Utilities.sleepAndProcessInterruptedException(100, logger);
      }
      Utilities.logWarn(logger, " ", Utilities.getCurrentThreadDescription(), " for ", serverName, " has stopped ");
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  // public static void main(String[] args) throws IOException {
  // mainInternal(args, true);
  // }
  //
  // public static DistributedServer mainInternal(String[] args, boolean inMainThread) throws IOException {
  // Utilities.parseVmArgs(args, null);
  // Configuration conf = Utilities.loadConfiguration("distributed-server");
  // if (conf.getBoolean("distributed.data.format", false)) {
  // stopServer(conf);
  // return startServer(conf, inMainThread);
  // } else if (conf.getBoolean("distributed.server.stop", false)) {
  // stopServer(conf);
  // return null;
  // } else {
  // return startServer(conf, inMainThread);
  // }
  // }

  public static void stopServer(Configuration conf) throws IOException {
    conf = new Configuration(conf);
    String serverName = getServerName(conf);
    conf.set("distributed.server.name", serverName);
    if (conf.getBoolean("distributed.data.format", false)) configLogger(conf);
    else {
      conf.set("distributed.logger.levels", "org.apache.zookeeper.ZooKeeper=warn,org.apache.zookeeper.ClientCnxn=warn");
      Utilities.setLoggerLevel(conf, null);
    }
    Utilities.logInfo(logger, serverName, " is stopping");

    // try to request distributed server to stop
    conf.setInt("ipc.client.connect.max.retries", 0);
    new DistributedMonitor(conf).stop();
    // kill process if distributed server is still running
    String[] includes = new String[] { serverName.replace("localhost", "127.0.0.1"), "java" };
    String[] fields = new String[] { "4", "7" };
    List<String> addressList = Utilities.getListenAddressList(includes, null, fields);
    for (String address : addressList) {
      if (address.split(",").length < 2) continue;
      String distributedServerPid = address.split(",")[1];
      if (distributedServerPid.split("/").length < 2) continue;
      distributedServerPid = distributedServerPid.split("/")[0];
      if (distributedServerPid.equals(Utilities.getPidString())) continue;
      Utilities.killProcess(distributedServerPid);
      Utilities.logInfo(logger, serverName, " with pid=", distributedServerPid, " is killed");
      break;
    }
    // kill all other processes which specified the data path in the command line
    String commandForKillRelativePids = "ps -ef| grep -v grep|grep " + getDataPath(conf) + "|awk '{print $2}'";
    String subProcessPids = Utilities.runCommand(commandForKillRelativePids, null, null, null).replaceAll("\n", ",");
    if (!subProcessPids.isEmpty()) {
      if (subProcessPids.charAt(subProcessPids.length() - 1) == ',')
        subProcessPids = subProcessPids.substring(0, subProcessPids.length() - 1);
      for (String pid : subProcessPids.split(",")) {
        if (pid.equals(Utilities.getPidString())) continue;
        Utilities.killProcess(pid);
        Utilities.logInfo(logger, serverName, "'s sub processes with pid=", subProcessPids, " is killed");
      }
    }
    Utilities.logInfo(logger, serverName, " has been stopped");
  }

  //
  // public static DistributedServer startServer(Configuration conf, boolean inMainThread) throws IOException {
  // if (inMainThread) {
  // DistributedServer distributedServer = new DistributedServer(conf);
  // distributedServer.waitUntilStop();
  // return distributedServer;
  // } else {
  // class DistributedServerThread extends Thread {
  // Configuration conf = null;
  // volatile DistributedServer distributedServer = null;
  //
  // DistributedServerThread(Configuration conf) {
  // this.conf = conf;
  // setDaemon(true);
  // setName(getClass().getSimpleName() + "@" + getName());
  // }
  //
  // public void run() {
  // try {
  // distributedServer = new DistributedServer(conf);
  // distributedServer.waitUntilStop();
  // } catch (Throwable t) {
  // Utilities.logError(logger, "server exits abnormally", t);
  // }
  // }
  //
  // public DistributedServer startServer() {
  // start();
  // while (true) {
  // if (distributedServer != null || !isAlive()) break;
  // Utilities.sleepAndProcessInterruptedException(1000, logger);
  // }
  // return distributedServer;
  // }
  // }
  // return new DistributedServerThread(conf).startServer();
  // }
  // }

  boolean openData() {
    try {
      data.open();
      return true;
    } catch (Throwable t) {
      log(Level.ERROR, " fail to open data", t);
      return false;
    }
  }

  boolean closeData() {
    try {
      data.close();
      return true;
    } catch (Throwable t) {
      log(Level.ERROR, " fail to close data ", t);
      return false;
    }
  }

  public Object dataInvokeInternal(Invocation dataInvocation) throws IOException {
    ServerType serverTypeSnapshot = null;
    List<ServerStatus> bakupServersSnapshot = null;
    synchronized (statusLock) {
      serverTypeSnapshot = serverType;
      bakupServersSnapshot = serverStatuses.get(true, ServerType.SLAVE, ServerType.UNDER_RESTORE);
    }

    if (!serverTypeSnapshot.is(ServerType.MASTER, ServerType.SLAVE, ServerType.UNDER_RESTORE)) {
      String message = serverName + "|" + serverTypeSnapshot + " refuse to do call " + dataInvocation;
      throw new DistributedException(true, new IOException(message));
    }

    // write data or edit log
    if (serverTypeSnapshot.is(ServerType.MASTER)) {
      try {
        data.invoke(dataInvocation);
        return dataInvocation.getResult();
      } catch (Throwable t) {
        dataInvocation.setResult(t);
        throw new IOException(t);
      } finally {
        log(Level.DEBUG, " do write call ", dataInvocation);
        writeBackupServers(dataInvocation, bakupServersSnapshot);
      }
    } else if (ServerType.SLAVE.equals(serverType) && !conf.getBoolean("distributed.server.write.async", false)) {
      data.invoke(dataInvocation);
      log(Level.DEBUG, " do write call ", dataInvocation);
      return dataInvocation.getResult();
    } else {
      long startTime = System.currentTimeMillis();
      dataInvocation.setResult(dataInvocation.getResult());
      int workSize = editLogger.append(dataInvocation);
      dataInvocation.setElapsedTime(System.currentTimeMillis() - startTime);
      log(Level.DEBUG, " append edit 1/", workSize, " log for write call ", dataInvocation);
      return dataInvocation.getResult();
    }
  }

  /**
   * master needs to write all slave and UNDER_RESTORE servers, when fail to write any server, we ensure that:
   * 1.all clients will read old value on any master/slave before one client requests master to do write call;
   * 2.all clients will read new value on any master/slave after the client is responded by the master.
   * NOTE: all clients will read old OR new value on any master/slave when master is responding the write request.
   */
  public void writeBackupServers(Invocation dataInvocation, List<ServerStatus> bakupServers) {
    long threadId = Thread.currentThread().getId();
    DistributedOperation[] operations = data.getOperationQueue().lockAndGetOperations(threadId);
    try {
      if (operations == null || operations.length == 0) return;
      if (dataInvocation == null || bakupServers == null || bakupServers.isEmpty()) return;
      dataInvocation.setDistributedOperations(operations);
      for (ServerStatus serverStatus : bakupServers) {
        try {
          long startTime = System.currentTimeMillis();
          serverStatus.proxy.invoke(getDataInvocation(dataInvocation));
          dataInvocation.setElapsedTime(System.currentTimeMillis() - startTime);
          DistributedMetrics.timeVaryingRateInc("dataInvokeBySlave." + dataInvocation.getMethodName(), dataInvocation
              .getElapsedTime());
          log(Level.DEBUG, " request ", serverStatus.name, "|", serverStatus.type, "|", serverStatus.version,
              " to do write call ", dataInvocation);
        } catch (Throwable t) {
          log(Level.WARN, " fail to request ", serverStatus.name, "|", serverStatus.type, "|", serverStatus.version,
              " to do write call ", dataInvocation, t);
          // register new type as NEED_RESTORE and wait until new type is applied or client lease is timeout
          register(serverStatus.name, -1L, ServerType.NEED_RESTORE);
        }
      }
    } finally {
      data.getOperationQueue().deleteAndUnlockOperations(threadId);
    }
  }

  long getLeaseTimeout() {
    return conf.getLong("distributed.lease.timeout", 1000);
  }

  public Object dataInvoke(Invocation invocation) throws IOException {
    try {
      // wait until editLogger has applied all edit logs for master
      if (serverType.is(ServerType.MASTER)) editLogger.waitUntilWorkSizeIsEmpty();
      getDataLocker.readLock().lock();

      long startTime = System.currentTimeMillis();
      invocation.setResult(dataInvokeInternal(invocation));
      if (invocation.getMethodName().equals("format")) editLogger.waitUntilWorkSizeIsEmpty();
      DistributedMetrics.timeVaryingRateIncWithStartTime("dataInvoke." + invocation.getMethodName(), startTime);
      return invocation.getResult();
    } catch (Throwable t) {
      if (DistributedException.isNeedRestore(t)) {
        log(Level.WARN, " fail to do ", invocation, t);
        if (serverType.is(ServerType.MASTER) && serverStatuses.getSlaves(false).isEmpty()) {
          try {
            closeData();
            openData();
          } catch (Throwable t2) {
            log(Level.WARN, " fail to close and reopen data", t2);
          }
        } else {
          register(serverName, data.getDataVersion(), ServerType.NEED_RESTORE);
          updateServers();
          throw new DistributedException(true, new IOException("master has changed, new type is " + serverType));
        }
      }
      throw new IOException(t);
    } finally {
      getDataLocker.readLock().unlock();
    }
  }

  static public Invocation getDataInvocation(Invocation dataInvocation) throws IOException {
    return new Invocation(DistributedServer.class, "dataInvoke", new Object[] { dataInvocation });
  }

  static public String getLoggerLevel(String loggerName) throws IOException {
    return Utilities.getLoggerLevel(loggerName);
  }

  public String[] getLoggerInfos() throws IOException {
    if (!conf.getBoolean("distributed.logger.levels.follow.enable", true)) return new String[0];
    List<String> loggerInfos = Utilities.getLoggerInfos();
    String[] loggersToFollow = conf.getStrings("distributed.logger.levels.follow.range", "com.taobao.adfs");
    List<String> loggerInfosToFollow = new ArrayList<String>();
    for (String loggerInfo : loggerInfos) {
      for (String loggerToFollow : loggersToFollow) {
        if (loggerToFollow != null && loggerInfo.startsWith(loggerToFollow)) {
          loggerInfosToFollow.add(loggerInfo);
          break;
        }
      }
    }
    return loggerInfosToFollow.toArray(new String[loggerInfosToFollow.size()]);
  }

  static String setLoggerLevel(String loggerName, String level) throws IOException {
    return Utilities.setLoggerLevel(loggerName, level, logger);
  }

  public String[] getConfSettings() throws IOException {
    Map<String, String> confMap = Utilities.getConf(conf, null);
    String[] confKeysToFollow = conf.getStrings("distributed.conf.follow.range", "distributed.metrics.");
    List<String> confsToFollow = new ArrayList<String>();
    for (String confKey : confMap.keySet()) {
      if (confKey == null || confKey.isEmpty() || confMap.get(confKey) == null) continue;
      for (String confKeyToFollow : confKeysToFollow) {
        if (confKeyToFollow != null && !confKeyToFollow.isEmpty() && confKey.startsWith(confKeyToFollow)) {
          confsToFollow.add(confKey + "=" + confMap.get(confKey));
          break;
        }
      }
    }
    return confsToFollow.toArray(new String[confsToFollow.size()]);
  }

  public int getServerPid() throws IOException {
    return Utilities.getPid();
  }

  private static final ThreadLocal<Invocation> threadLocalInvocation = new ThreadLocal<Invocation>();

  public static ThreadLocal<Invocation> getThreadLocalInvocation() {
    return threadLocalInvocation;
  }

  public static Invocation getCurrentInvocation() {
    Invocation currentInvocation = threadLocalInvocation.get();
    if (currentInvocation == null) {
      currentInvocation = new Invocation();
      try {
        currentInvocation.setCallerAddress(IpAddress.getAddress("127.0.0.1"));
      } catch (IOException e) {
        // never come to here
        e.printStackTrace();
      }
      currentInvocation.setCallerProcessId(Utilities.getPid());
      currentInvocation.setCallerThreadId(Thread.currentThread().getId());
      currentInvocation.setCallerThreadName(Thread.currentThread().getName());
      currentInvocation.setCallerSequenceNumber(0);
      threadLocalInvocation.set(currentInvocation);
    }

    return currentInvocation;
  }

  public static String getCurrentClientName() {
    Invocation currentInvocation = getCurrentInvocation();
    if (currentInvocation == null) return null;
    return IpAddress.getAddress(currentInvocation.getCallerAddress()) + "-" + currentInvocation.getCallerProcessId();
  }

  /**
   * call public/protected/private/default method of this server
   */
  public Object invoke(Invocation invocation) throws IOException {
    long startTime = System.currentTimeMillis();
    try {
      threadLocalInvocation.set(invocation);
      invocation.setCallerAddress(IpAddress.getAddress(RPC.Server.getRemoteAddress()));
      if ("dataInvoke".equals(invocation.getMethodName())) {
        Invocation dataInvocation = ((Invocation) (invocation.getParameters()[0]));
        if (dataInvocation == null) throw new IOException("data invocation is null");
        dataInvocation.setCallerAddress(invocation.getCallerAddress());
        dataInvocation.setCallerProcessId(invocation.getCallerProcessId());
        dataInvocation.setCallerThreadId(invocation.getCallerThreadId());
        dataInvocation.setCallerThreadName(invocation.getCallerThreadName());
        dataInvocation.setCallerSequenceNumber(invocation.getCallerSequenceNumber());
      }
      while (true) {
        Object result = invocation.invoke(this);
        if (DistributedResult.needRetry(result) && "dataInvoke".equals(invocation.getMethodName())) {
          log(Level.WARN, " retry to call ", invocation);
          Invocation dataInvocation = ((Invocation) (invocation.getParameters()[0]));
          if (dataInvocation == null) throw new IOException("data invocation is null");
          dataInvocation.resetResult();
          invocation.resetResult();
          continue;
        } else return result;
      }
    } finally {
      DistributedMetrics.timeVaryingRateIncWithStartTime("invoke." + invocation.getMethodName(), startTime);
      log(Level.TRACE, " spend ", System.currentTimeMillis() - startTime, "ms to invoke for ", invocation
          .getIdentifier());
    }
  }

  public Object getFieldValue(String fieldName) throws IOException {
    return Utilities.getFieldValue(this, fieldName);
  }

  public void setFieldValue(String fieldName, Object fieldValue) throws IOException {
    Utilities.setFieldValue(this, fieldName, fieldValue);
  }

  public Object getFieldTypeName(String fieldName) throws IOException {
    Object fieldValue = getFieldValue(fieldName);
    if (fieldValue != null) return fieldValue.getClass().getName();
    else return Utilities.getField(getClass(), fieldName).getType().getName();
  }

  /**
   * call method of field with name of fieldName
   */
  public Object invoke(String fieldName, String methodName, Object[] args) throws IOException {
    return new Invocation(getFieldValue(fieldName), methodName, args).invoke();
  }

  /**
   * call public field with name of subFieldName of field with name of fieldName
   */
  public Object invoke(String fieldName, String subFieldName) throws IOException {
    return Utilities.getFieldValue(Utilities.getFieldValue(this, fieldName), subFieldName);
  }

  static public enum ServerType {
    MASTER, SLAVE, UNDER_RESTORE, NEED_RESTORE, ONLINE, STANDBY, STOP;
    public boolean is(ServerType... serverTypes) {
      for (ServerType serverType : serverTypes) {
        if (equals(serverType)) return true;
      }
      return false;
    }

    public boolean not(ServerType... serverTypes) {
      for (ServerType serverType : serverTypes) {
        if (equals(serverType)) return false;
      }
      return true;
    }
  }

  static public class ServerStatus {
    public String name;
    public Long version;
    public ServerType type;
    public String setterName;
    @LongTime("")
    public Long createTime;
    public Long createSerialNumber;
    @LongTime("")
    public Long modifyTime;
    public DistributedInvocable proxy;
    private String statusProcess;

    public ServerStatus(String name, Long version, ServerType type, String setterName) {
      this.name = name;
      this.version = version;
      this.type = type;
      this.setterName = setterName;
    }

    public ServerStatus(String name, Long version, ServerType type, String setterName, Long createTime,
        Long createSerialNumber, Long modifyTime) {
      this.name = name;
      this.version = version;
      this.type = type;
      this.setterName = setterName;
      this.createTime = createTime;
      this.createSerialNumber = createSerialNumber;
      this.modifyTime = modifyTime;
    }

    /**
     * type is changed but has not been registered to the manager
     */
    public boolean isTypeRegistered() {
      return setterName == null;
    }

    /**
     * type is registered to the manager but has not been applied
     */
    public boolean isTypeApplied() {
      return name.equals(setterName);
    }

    public String toString() {
      statusProcess = setterName == null ? "unregistered" : setterName.equals(name) ? "APPLIED" : "APPLYING";
      return Utilities.toStringByFields(this, false);
    }

    /**
     * name|type|version|APPLIED
     */
    public String toStringWithNameVersionTypeProcess() {
      statusProcess = setterName == null ? "unregistered" : setterName.equals(name) ? "APPLIED" : "APPLYING";
      return new StringBuilder().append(name).append('|').append(version).append('|').append(type).append('|').append(
          statusProcess).toString();
    }

    static public Collection<ServerStatus> sortByTypeAndName(Collection<ServerStatus> serverStatuses) {
      if (serverStatuses == null) return null;
      List<ServerStatus> serverStatusList = new ArrayList<ServerStatus>(serverStatuses);
      serverStatuses.clear();
      for (ServerType serverType : ServerType.values()) {
        List<ServerStatus> serverStatusListWithSpecifiedType = new ArrayList<ServerStatus>();
        for (ServerStatus serverStatus : serverStatusList) {
          if (serverStatus != null && serverType.equals(serverStatus.type))
            serverStatusListWithSpecifiedType.add(serverStatus);
        }
        while (!serverStatusListWithSpecifiedType.isEmpty()) {
          ServerStatus serverStatusWithSmallestName = null;
          for (ServerStatus serverStatusWithSpecifiedType : serverStatusListWithSpecifiedType) {
            if (serverStatusWithSmallestName == null
                || serverStatusWithSpecifiedType.name.compareTo(serverStatusWithSmallestName.name) < 0) {
              serverStatusWithSmallestName = serverStatusWithSpecifiedType;
            }
          }
          serverStatuses.add(serverStatusWithSmallestName);
          serverStatusListWithSpecifiedType.remove(serverStatusWithSmallestName);
        }
      }

      return serverStatuses;
    }
  }
}
