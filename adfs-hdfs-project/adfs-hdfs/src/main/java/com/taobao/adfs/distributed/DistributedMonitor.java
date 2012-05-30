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
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedServer.ServerStatus;
import com.taobao.adfs.distributed.DistributedServer.ServerType;
import com.taobao.adfs.distributed.rpc.RPC;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 * @created 2011-05-17
 */
public class DistributedMonitor {
  public static final Logger logger = LoggerFactory.getLogger(DistributedMonitor.class);
  Configuration conf = null;
  protected Object operationProxy = null;

  public DistributedMonitor(Configuration conf) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : conf;
  }

  public DistributedMonitor(Configuration conf, String serverName) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : new Configuration(conf);
    this.conf.set("distributed.server.name", serverName);
  }

  private DistributedInvocable createServerProxy(String serverName, Configuration conf) throws IOException {
    return (DistributedInvocable) RPC.getProxy(DistributedInvocable.class, DistributedInvocable.versionID, NetUtils
        .createSocketAddr(serverName), conf);
  }

  public boolean stop() throws IOException {
    DistributedManager distributedManager = new DistributedManager(conf);
    ServerStatus serverStatus = distributedManager.getServers(null).get(getServerName());
    if (serverStatus != null && !serverStatus.type.is(ServerType.STOP))
      distributedManager.register(getServerName(), -1, ServerType.STOP, getClass().getSimpleName());
    // wait until server has been stop
    Configuration confForCreateServerProxy = new Configuration(conf);
    confForCreateServerProxy.set("ipc.client.connect.max.retries", "0");
    for (int i = 0; i < 10; ++i) {
      serverStatus = distributedManager.getServers(null).get(getServerName());
      if (serverStatus == null || (serverStatus.type.is(ServerType.STOP) && serverStatus.isTypeApplied())) {
        try {
          RPC.stopProxy(createServerProxy(getServerName(), confForCreateServerProxy));
        } catch (IOException e) {
          return true;
        }
      }
      Utilities.sleepAndProcessInterruptedException(1000, logger);
    }
    return false;
  }

  public Class<?>[] getDataProtocols() throws IOException {
    return (Class<?>[]) remoteFieldMethod("data", "getDataProtocols");
  }

  public ServerType getServerType() throws IOException {
    return (ServerType) getRemoteField("serverType");
  }

  public int getServerPid() throws IOException {
    return (Integer) remoteMethod("getServerPid");
  }

  public Long getLeaseTimeout() throws IOException {
    return (Long) remoteMethod("getLeaseTimeout");
  }

  String getServerName() {
    return conf.get("distributed.server.name");
  }

  public void register(ServerType serverType) throws IOException {
    if (serverType == null) throw new IOException("serverType is null");
    if (serverType.is(ServerType.STOP)) {
      stop();
      return;
    }
    DistributedManager distributedManager = new DistributedManager(conf);
    String name = getServerName();
    long version = -1;
    ServerStatus serverStatus = distributedManager.getServerStatus(getServerName());
    if (serverStatus != null) version = serverStatus.version;
    String setterName = getClass().getSimpleName();
    for (int i = 0; i < 100; ++i) {
      distributedManager.register(name, version, serverType, setterName);
      serverStatus = distributedManager.getServerStatus(name);
      if (serverStatus != null) {
        if (serverStatus.type.equals(serverType) && serverStatus.isTypeApplied()) return;
        version = serverStatus.version;
      }
      Utilities.sleepAndProcessInterruptedException(100, logger);
    }
    throw new IOException("fail to register " + name + " with new serverType=" + serverType);
  }

  public void unregister() throws IOException {
    DistributedManager distributedManager = new DistributedManager(conf);
    distributedManager.unregister(getServerName());
  }

  public String getLoggerLevel(String loggerName) throws IOException {
    return (String) remoteMethod("getLoggerLevel", loggerName);
  }

  public String[] getLoggerInfos() throws IOException {
    return (String[]) remoteMethod("getLoggerInfos");
  }

  public String setLoggerLevel(String loggerName, String level) throws IOException {
    return (String) remoteMethod("setLoggerLevel", loggerName, level);
  }

  public String[] getConfSettings() throws IOException {
    return (String[]) remoteMethod("getConfSettings");
  }

  public String getConf(String key) throws IOException {
    return (String) remoteFieldMethod("conf", "get", key);
  }

  public void setConf(String key, String value) throws IOException {
    remoteFieldMethod("conf", "set", key, value);
  }

  public long getDataVersion() throws IOException {
    return (Long) remoteFieldMethod("data", "getDataVersion");
  }

  public String getDataLocker() throws IOException {
    return remoteFieldMethod("data", "getDataLocker").toString();
  }

  public String getDataRepositoryLockers() throws IOException {
    return Utilities.deepToString(remoteFieldMethod("data", "getDataRepositoryLockers"));
  }

  public Object getRemoteField(String fieldName) throws IOException {
    return remoteMethod("getFieldValue", fieldName);
  }

  public void setRemoteField(String fieldName, Object value) throws IOException {
    remoteMethod("setFieldValue", 1, fieldName, value);
  }

  public Object remoteMethod(String methodName, Object... parameters) throws IOException {
    return remoteMethod(methodName, -1, parameters);
  }

  public Object remoteMethod(String methodName, Integer realTypeIndication, Object... parameters) throws IOException {
    String serverName = conf.get("distributed.server.name");
    serverName = serverName.replaceFirst("0.0.0.0", "localhost");
    DistributedInvocable serverProxy = createServerProxy(serverName, conf);
    try {
      Class<?>[] parameterTypes = new Class[parameters.length];
      for (int i = 0; i < parameters.length; ++i) {
        if (((realTypeIndication >> i) & 1) == 0 || parameters[i] == null) parameterTypes[i] = Object.class;
        else parameterTypes[i] = parameters[i].getClass();
      }
      Method method = Invocation.getMethod(DistributedServer.class, methodName, parameterTypes);
      Invocation invocation = new Invocation(method, parameters);
      invocation.setResult(serverProxy.invoke(invocation));
      Utilities.logDebug(logger, "request " + serverName + " to call " + invocation);
      return invocation.getResult();
    } finally {
      RPC.stopProxy(serverProxy);
    }
  }

  public Object remoteFieldMethod(String fieldName, String methodName, Object... parameters) throws IOException {
    return remoteMethod("invoke", fieldName, methodName, parameters);
  }

  public Object remoteFieldField(String fieldName, String subFieldName) throws IOException {
    return remoteMethod("invoke", fieldName, subFieldName);
  }

  public String remoteFieldTypeName(String fieldName) throws IOException {
    return (String) remoteMethod("getFieldTypeName", fieldName);
  }
}
