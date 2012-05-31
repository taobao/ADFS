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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedServer.ServerStatus;
import com.taobao.adfs.distributed.metrics.DistributedMetrics;
import com.taobao.adfs.distributed.rpc.ClassCache;
import com.taobao.adfs.distributed.rpc.RPC;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.distributed.rpc.RemoteException;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedClient implements Closeable, InvocationHandler {
  public static final Logger logger = LoggerFactory.getLogger(DistributedClient.class);
  ServerStatus masterServer = null;
  DistributedManager serverManager = null;
  DistributedMonitor distributedMonitor = null;
  Configuration conf = null;
  protected Object operationProxy = null;
  DistributedLeaseThread distributedLeaseThread = null;
  static AtomicLong invocationCounter = new AtomicLong(0);
  public static DistributedMetrics distributedMetrics = null;

  static public Closeable getClient(Configuration conf) throws IOException {
    return (Closeable) new DistributedClient(conf).operationProxy;
  }

  static public Closeable getClient(Configuration conf, boolean waitServer) throws IOException {
    conf.setBoolean("distributed.client.wait.server", waitServer);
    return getClient(conf);
  }

  static public void close(Object distributedClient) {
    try {
      ((Closeable) distributedClient).close();
    } catch (Throwable t) {
      Utilities.logWarn(logger, "fail to close client", t);
    }
  }

  static public DistributedClient getClient(Object proxy) {
    try {
      return (DistributedClient) Utilities.getFieldValue(proxy, "h");
    } catch (Throwable t) {
      Utilities.logWarn(logger, "fail to get handler for proxy", t);
      return null;
    }
  }

  static public InetSocketAddress getMasterAddress(Object proxy) {
    DistributedClient client = getClient(proxy);
    if (client == null) return null;
    else return client.getMasterAddress();
  }

  private DistributedClient(Configuration conf) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : conf;
    initialize();
  }

  private void initialize() throws IOException {
    Utilities.setLoggerLevel(conf, logger);
    waitMaster();
    createClientProxy();
    updateServers();
    followConfSettings();
    followLoggerLevels();
    createMetrics();
    distributedLeaseThread = createLeaseThread();
  }

  void createMetrics() throws IOException {
    if (!conf.getBoolean("distributed.metrics.enable", true)) return;
    distributedMetrics = new DistributedMetrics(conf);
    distributedMetrics.open("client");
  }

  void createClientProxy() throws IOException {
    Class<?>[] dataInterfaces = getDistributedDataProtocols(conf);
    Class<?>[] interfaces = new Class<?>[dataInterfaces.length + 1];
    for (int i = 0; i < dataInterfaces.length; ++i) {
      interfaces[i] = dataInterfaces[i];
    }
    interfaces[dataInterfaces.length] = Closeable.class;

    operationProxy = Proxy.newProxyInstance(getClass().getClassLoader(), interfaces, this);
  }

  public ServerStatus getMaster() {
    return masterServer;
  }

  public InetSocketAddress getMasterAddress() {
    if (masterServer == null || masterServer.name == null) return null;
    String host = Utilities.getHost(masterServer.name);
    Integer port = Utilities.getPort(masterServer.name);
    if (host == null || port == null) return null;
    return InetSocketAddress.createUnresolved(host, port);
  }

  public Class<?>[] getDistributedDataProtocols(Configuration conf) throws IOException {
    conf.set("distributed.client.enabled", "true");
    try {
      String dataClassName = conf.get("distributed.data.class.name");
      if (dataClassName != null) return ClassCache.get(dataClassName).getInterfaces();
    } catch (Throwable t) {
      Utilities.logWarn(logger, "fail to load data class", t);
    }

    for (ServerStatus serverStatus : getServerManager().getServers(null).getNotStop(false)) {
      try {
        if (serverStatus != null) return new DistributedMonitor(conf, serverStatus.name).getDataProtocols();
      } catch (Throwable t) {
        Utilities.logWarn(logger, "fail to request ", serverStatus.name, " to get data protocols");
      }
    }
    throw new IOException("no server to get distributed.data.class.name");
  }

  public static String getDistributedDataTypeName(Configuration conf) throws IOException {
    conf.set("distributed.client.enabled", "true");
    String dataClassName = conf.get("distributed.data.class.name");
    if (dataClassName != null) return dataClassName;
    for (ServerStatus serverStatus : new DistributedManager(conf).getServers(null).get()) {
      try {
        if (serverStatus != null) return new DistributedMonitor(conf, serverStatus.name).remoteFieldTypeName("data");
      } catch (Throwable t) {
        continue;
      }
    }
    throw new IOException("no server to get distributed.data.class.name");
  }

  DistributedLeaseThread createLeaseThread() {
    DistributedLeaseThread distributedLeaseThread = new DistributedLeaseThread();
    distributedLeaseThread.start();
    return distributedLeaseThread;
  }

  private DistributedInvocable createServerProxy(String serverName) throws IOException {
    if (serverName == null) throw new IOException("fail to create server proxy for serverName=" + null);
    DistributedInvocable distributedProxy =
        (DistributedInvocable) RPC.getProxy(DistributedInvocable.class, DistributedInvocable.versionID, NetUtils
            .createSocketAddr(serverName), conf);
    return distributedProxy;
  }

  synchronized DistributedManager getServerManager() throws IOException {
    if (serverManager == null) serverManager = new DistributedManager(conf);
    return serverManager;
  }

  @Override
  public void close() {
    Utilities.logDebug(logger, "start to close client");
    if (distributedLeaseThread != null) distributedLeaseThread.close();
    if (masterServer != null && masterServer.proxy != null) RPC.stopProxy(masterServer.proxy);
    Utilities.logDebug(logger, "succeed in closing client");
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (method.getName().equals("close") && method.getDeclaringClass().equals(Closeable.class)) {
      close();
      return null;
    }

    Invocation invocation = DistributedServer.getDataInvocation(new Invocation(method, args));
    invocation.setCallerProcessId(Utilities.getPid());
    invocation.setCallerThreadId(Thread.currentThread().getId());
    invocation.setCallerThreadName(Thread.currentThread().getName());
    invocation.setCallerSequenceNumber(invocationCounter.getAndIncrement());

    int retryNumber = conf.getInt("distributed.client.retry.number", 60);
    int retrySleepTime = conf.getInt("distributed.client.retry.sleep", 10000);
    for (int i = 0; i < retryNumber; ++i) {
      try {
        long startTime = System.currentTimeMillis();
        if (masterServer == null) throw new IOException("no master to do call " + invocation);
        if (masterServer.proxy == null) throw new IOException("no proxy to " + masterServer.name);
        invocation.setResult(masterServer.proxy.invoke(invocation));
        invocation.setElapsedTime(System.currentTimeMillis() - startTime);
        Utilities.logDebug(logger, "request ", masterServer.name, " to do ", invocation);
        if (distributedMetrics != null)
          distributedMetrics.getMetricsTimeVaryingRate("dataInvoke." + method.getName()).inc(
              invocation.getElapsedTime());
        return invocation.getResult();
      } catch (Throwable t) {
        if (t instanceof RemoteException
            && ((!DistributedException.isNeedRestore(t) && !DistributedException.isRefuseCall(t)))) {
          // part data is written and thrown an exception when write remaining data
          Utilities.logInfo(logger, "request ", masterServer.name, " to do ", invocation, " exception=", t);
          throw t;
        }
        Utilities.logWarn(logger, "fail to request ", masterServer.name, " to do ", invocation, ", retryIndex=", i,
            ", maxRetryIndex=", retryNumber - 1, " exception=", t);
        if (i < retryNumber - 1) Utilities.sleepAndProcessInterruptedException(retrySleepTime, logger);
        else throw t;
      }
    }
    throw new IOException("never to here");
  }

  boolean isProxyValid(DistributedInvocable proxy) {
    if (proxy == null) return false;
    try {
      proxy.getProtocolVersion(DistributedInvocable.class.getName(), DistributedInvocable.versionID);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  synchronized void waitMaster() throws IOException {
    if (!conf.getBoolean("distributed.client.wait.master", true)) return;
    long startTime = System.currentTimeMillis();
    while (true) {
      try {
        ServerStatus masterServer = getServerManager().getServers(null).getMaster(true);
        if (masterServer == null) throw new IOException("master server is null@" + getServerManager().getAddress());
        if (isProxyValid(masterServer.proxy)) throw new IOException("master server proxy is invalid");
        return;
      } catch (Throwable t) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        long waitTimeout = conf.getLong("distributed.client.wait.master.timeout", Long.MAX_VALUE);
        if (elapsedTime > waitTimeout) throw new IOException("fail to get master server and wait is timeout", t);
        Utilities.logInfo(logger, "fail to get master server, willl retry, exception=", t);
        long sleepTime = conf.getLong("distributed.client.wait.master.sleep", 1000);
        Utilities.sleepAndProcessInterruptedException(sleepTime, logger);
      }
    }
  }

  private synchronized ServerStatus updateServers() throws IOException {
    ServerStatus newMasterServer = getServerManager().getServers(null).getMaster(true);
    if (newMasterServer == null) {
      if (masterServer == null || isProxyValid(masterServer.proxy)) return masterServer;
      else {
        DistributedInvocable oldProxy = masterServer.proxy;
        masterServer.proxy = null;
        RPC.stopProxy(oldProxy);
        Utilities.logInfo(logger, "stop proxy to ", masterServer.name);
      }
    }
    if (masterServer == null || masterServer.name == null || !masterServer.name.equals(newMasterServer.name)
        || !isProxyValid(masterServer.proxy)) {
      if (masterServer != null && masterServer.proxy != null) {
        RPC.stopProxy(masterServer.proxy);
        masterServer.proxy = null;
        Utilities.logInfo(logger, "stop proxy to ", masterServer.name);
        if (masterServer.name.equals(newMasterServer.name)) return masterServer;
      }
      newMasterServer.proxy = createServerProxy(newMasterServer.name);
      masterServer = newMasterServer;
      Utilities.logInfo(logger, "create proxy to ", masterServer.name);
    }
    return masterServer;
  }

  public synchronized void followConfSettings() throws IOException {
    if (masterServer == null || masterServer.proxy == null) return;
    String excludeString = conf.get("distributed.conf.follow.excludes");
    if (excludeString == null) excludeString = "";
    if (!Utilities.stringEquals("distributed.conf.follow.excludes", excludeString))
      excludeString = "distributed.conf.follow.excludes," + excludeString;
    if (!Utilities.stringEquals("distributed.logger.follow.excludes", excludeString))
      excludeString = "distributed.conf.follow.excludes," + excludeString;
    conf.set("distributed.conf.follow.excludes", excludeString);
    String[] excludes = conf.getStrings("distributed.conf.follow.excludes");
    String[] confSettings = new DistributedMonitor(conf, masterServer.name).getConfSettings();
    for (String confSetting : confSettings) {
      String[] keyAndValue = confSetting.split("=", 2);
      if (keyAndValue.length < 2) continue;
      String key = keyAndValue[0];
      if (Utilities.stringStartsWith(key, excludes)) continue;
      String valueOfServer = keyAndValue[1];
      String valueOfClient = conf.get(key);
      if (valueOfServer != null && !valueOfServer.equals(valueOfClient)) {
        conf.set(key, valueOfServer);
        Utilities.logInfo(logger, "follow conf", "|key=", key, "|value=", valueOfClient, "--->", valueOfServer);
      }
    }
  }

  public synchronized void followLoggerLevels() throws IOException {
    if (masterServer == null || masterServer.proxy == null) return;
    String[] excludes = conf.getStrings("distributed.logger.follow.excludes");
    String[] loggerInfos = new DistributedMonitor(conf, masterServer.name).getLoggerInfos();
    for (String loggerInfo : loggerInfos) {
      String[] nameAndLevel = loggerInfo.split("=", 2);
      if (nameAndLevel.length < 2) continue;
      String name = nameAndLevel[0];
      if (Utilities.stringStartsWith(name, excludes)) continue;
      String level = nameAndLevel[1];
      Level levelOfClient = Level.toLevel(Utilities.getLoggerLevel(name), Level.INFO);
      Level levelOfServer = Level.toLevel(level, Level.INFO);
      if (!levelOfClient.equals(levelOfServer)) {
        Utilities.setLoggerLevel(name, levelOfServer.toString(), null);
        Utilities.logInfo(logger, "follow logger", "|name=", name, "|level=", levelOfClient, "--->", levelOfServer);
      }
    }
  }

  class DistributedLeaseThread extends Thread {
    AtomicBoolean shouldClose = new AtomicBoolean(false);
    AtomicBoolean isClosed = new AtomicBoolean(false);

    DistributedLeaseThread() {
      setDaemon(true);
      setName(getClass().getSimpleName() + "@" + getName());
    }

    public void close() {
      shouldClose.set(true);
      Utilities.waitValue(isClosed, true, null);
    }

    public void run() {
      while (true) {
        try {
          updateServers();
          followConfSettings();
          followLoggerLevels();
        } catch (Throwable t) {
          Utilities.logWarn(logger, "fail to update distributed lease ", t);
        } finally {
          for (int i = 0; i < conf.getInt("distributed.client.lease.time", 1000); i += 10) {
            if (shouldClose.get()) {
              isClosed.set(true);
              break;
            }
            Utilities.sleepAndProcessInterruptedException(10, logger);
          }
        }
      }
    }
  }
}
