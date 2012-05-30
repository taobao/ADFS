/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 \*   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.zookeeper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class ZookeeperClientManager {
  public static final Logger logger = LoggerFactory.getLogger(ZookeeperClientManager.class);
  private static Map<String, ZooKeeper> zookeeperRepository = new HashMap<String, ZooKeeper>();

  static public synchronized ZooKeeper create(String connectString, int sessionTimeout) throws IOException {
    ZooKeeper zookeeper = zookeeperRepository.get(connectString);
    if (zookeeper != null) {
      Utilities.logDebug(logger, "reuse zookeeper client, address=", connectString);
      return zookeeper;
    }

    Watcher watcher = new Watcher() {
      public void process(WatchedEvent event) {
      }
    };
    zookeeper = new ZooKeeper(connectString, sessionTimeout, watcher);
    zookeeperRepository.put(connectString, zookeeper);
    Utilities.logDebug(logger, "create zookeeper client, address=", connectString);
    return zookeeper;
  }

  static public synchronized void close(String connectString) {
    try {
      ZooKeeper zookeeper = zookeeperRepository.remove(connectString);
      if (zookeeper != null) zookeeper.close();
      Utilities.logDebug(logger, "close zookeeper client, address=", connectString);
    } catch (Throwable t) {
      Utilities.logWarn(logger, "fail to close client, address=", connectString, t);
    }
  }
}