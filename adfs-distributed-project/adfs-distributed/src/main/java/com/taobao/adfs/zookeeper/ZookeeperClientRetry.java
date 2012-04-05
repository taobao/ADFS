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

package com.taobao.adfs.zookeeper;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.OperationTimeoutException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.KeeperException.SessionMovedException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedManager;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class ZookeeperClientRetry {
  public static final Logger logger = LoggerFactory.getLogger(DistributedManager.class);
  String addresses;
  int sessionTimeout;
  int retryTimes;
  long retryDelayTime;

  public ZookeeperClientRetry(String addresses, int sessionTimeout, int retryTimes, long retryDelayTime)
      throws IOException {
    this.addresses = addresses;
    this.sessionTimeout = sessionTimeout;
    this.retryTimes = retryTimes;
    this.retryDelayTime = retryDelayTime;
  }

  ZooKeeper getZookeeperClient() throws IOException {
    return ZookeeperClientManager.create(addresses, sessionTimeout);
  }

  void closeZookeeperClient() {
    ZookeeperClientManager.close(addresses);
  }

  public int getSessionTimeout() throws IOException {
    Class<?>[] parameterClasses = new Class<?>[] {};
    return (Integer) retryInvoke(parameterClasses);
  }

  public Stat exists(String path) throws IOException {
    Class<?>[] parameterClasses = new Class<?>[] { String.class, boolean.class };
    return (Stat) retryInvoke(parameterClasses, path, false);
  }

  public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws IOException {
    try {
      Class<?>[] parameterClasses = new Class<?>[] { String.class, byte[].class, List.class, CreateMode.class };
      retryInvoke(parameterClasses, path, data, acl, createMode);
    } catch (Throwable t) {
      if (Utilities.getFirstCause(t) instanceof NodeExistsException) return;
      throw new IOException(t);
    }
  }

  public void delete(String path) throws IOException {
    Class<?>[] parameterClasses = new Class<?>[] { String.class, int.class };
    retryInvoke(parameterClasses, path, -1);
  }

  @SuppressWarnings("unchecked")
  public List<String> getChildren(String path) throws IOException {
    Class<?>[] parameterClasses = new Class<?>[] { String.class, boolean.class };
    return (List<String>) retryInvoke(parameterClasses, path, false);
  }

  public byte[] getData(String path, Stat stat) throws IOException {
    Class<?>[] parameterClasses = new Class<?>[] { String.class, boolean.class, Stat.class };
    return (byte[]) retryInvoke(parameterClasses, path, false, stat);
  }

  public Stat setData(String path, byte[] data) throws IOException {
    Class<?>[] parameterClasses = new Class<?>[] { String.class, byte[].class, int.class };
    return (Stat) retryInvoke(parameterClasses, path, data, -1);
  }

  public Object retryInvoke(Class<?>[] parameterClasses, Object... parameters) throws IOException {
    Throwable throwable = null;
    String methodName = Utilities.getCallerName();
    for (int i = 0; i < retryTimes; ++i) {
      try {
        return new Invocation(getZookeeperClient(), methodName, parameterClasses, parameters).invoke();
      } catch (Throwable t) {
        if (!isRetryThrowable(t, IOException.class, ConnectionLossException.class, SessionExpiredException.class,
            OperationTimeoutException.class, SessionExpiredException.class, SessionMovedException.class))
          throw new IOException(t);
        closeZookeeperClient();
        throwable = t;
        Utilities.logWarn(logger, " fail to request zookeeper retryIndex=", i, t);
        Utilities.sleepAndProcessInterruptedException(retryDelayTime, logger);
      }
    }
    throw new IOException(throwable);
  }

  public boolean isRetryThrowable(Throwable t, Class<?>... retryThrowableClasses) {
    if (t == null) return false;
    t = Utilities.getFirstCause(t);
    for (Class<?> retryThrowableClass : retryThrowableClasses) {
      if (t.getClass().equals(retryThrowableClass)) return true;
    }
    return false;
  }
}
