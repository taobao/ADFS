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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedServer.ServerStatus;
import com.taobao.adfs.distributed.DistributedServer.ServerType;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.Utilities;
import com.taobao.adfs.zookeeper.ZookeeperClientRetry;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 * @created 2011-05-17
 */
public class DistributedManager {
  public static final Logger logger = LoggerFactory.getLogger(DistributedManager.class);
  public static final String parentPathOfManagerName = "/distributed";
  String name = null;
  Configuration conf = null;
  ZookeeperClientRetry zookeeperClientRetry;

  public DistributedManager(Configuration conf) throws IOException {
    this.conf = (conf == null) ? new Configuration(false) : conf;
    initialize();
  }

  void initialize() throws IOException {
    invocationToGetStatusForLog = new Invocation(this, "getStatus");
    zookeeperClientRetry =
        new ZookeeperClientRetry(getAddress(), getManagerTimeout(), getRetryTimes(), getRetryDelayTime());
    setName();
  }

  synchronized public String setName() throws IOException {
    if (name != null && !name.isEmpty()) return name;
    name = conf.get("distributed.manager.name");
    if (name != null && !name.isEmpty()) return name;
    name = conf.get("distributed.data.client.class.name");
    if (name != null && !name.isEmpty()) {
      if (name.contains(".")) name = name.split("\\.")[name.split("\\.").length - 1];
      return name;
    }
    name = DistributedData.getDataClientClassName(conf.get("distributed.data.class.name"));
    if (name != null && !name.isEmpty()) {
      if (name.contains(".")) name = name.split("\\.")[name.split("\\.").length - 1];
      return name;
    }
    if (conf.getBoolean("distributed.client.enabled", true)) {
      List<String> nameList = getNameList();
      if (!nameList.isEmpty()) {
        name = nameList.get(0);
        log(Level.WARN, " randomly choose name=", name, " in ", Utilities.deepToString(nameList));
        return name;
      }
    }
    throw new IOException("need to specify distributed.manager.name or start one distributed server at least");
  }

  public String getPath() {
    return (name == null || name.isEmpty()) ? null : parentPathOfManagerName + "/" + name;
  }

  public String getAddress() {
    return conf.get("distributed.manager.address", "localhost:2181");
  }

  public String getName() {
    return name;
  }

  synchronized public List<String> getNameList() throws IOException {
    try {
      createNodeIfNotExist(parentPathOfManagerName);
      return zookeeperClientRetry.getChildren(parentPathOfManagerName);
    } catch (Throwable t) {
      throw new IOException(" fail to get manager name list", t);
    }
  }

  synchronized public long getSessionTimeout() throws IOException {
    return zookeeperClientRetry.getSessionTimeout();
  }

  public int getManagerTimeout() {
    return conf.getInt("distributed.manager.timeout", 30000);
  }

  public long getElectionDelayTime() {
    long electionDelayTime = conf.getLong("distributed.manager.election.delay.time", Long.MAX_VALUE);
    if (electionDelayTime < 0) electionDelayTime = Long.MAX_VALUE;
    return electionDelayTime;
  }

  public int getElectionServersMin() {
    return conf.getInt("distributed.manager.election.servers.min", 2);
  }

  public int getRetryTimes() {
    return conf.getInt("distributed.manager.retry.times", 100);
  }

  public int getRetryDelayTime() {
    return conf.getInt("distributed.manager.retry.delay.time", 1000);
  }

  synchronized void createNodeIfNotExist(String path) throws IOException {
    // create parent node
    String parentPath = new File(path).getParent();
    if (parentPath != null && !parentPath.equals("/")) createNodeIfNotExist(parentPath);

    // create end-point node
    try {
      if (zookeeperClientRetry.exists(path) == null)
        zookeeperClientRetry.create(path, path.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  /**
   * @param serverName
   *          for server: its name, for client: null
   */
  synchronized public ServerStatuses getServers(String serverName) {
    ServerStatuses serverStatuses = getServerStatuses();

    // return directly if caller is a client
    if (serverName == null) return serverStatuses;

    ServerStatus master = serverStatuses.getMaster(false);
    if (master == null) {
      // elect new master if caller is a server and no master is found
      master = serverStatuses.electMaster(this);
      // return directly if no master is elected or caller is not the new master
      if (master == null || !serverName.equals(master.name)) return serverStatuses;
      // set new type for all related servers if caller is the new master
      for (ServerStatus serverStatus : serverStatuses.getNot(false, ServerType.STANDBY, ServerType.STOP)) {
        if (serverStatus.name.equals(master.name)) serverStatus.type = ServerType.MASTER;
        else serverStatus.type = ServerType.NEED_RESTORE;
        serverStatus.setterName = null;
      }
      return serverStatuses;
    } else {
      // caller is a server and a master is found
      if (master.name.equals(serverName)) {
        // caller is a server and is the master, master needs to change ONLINE server to be NEED_RESTORE server
        for (ServerStatus serverStatus : serverStatuses.get(false, ServerType.ONLINE)) {
          serverStatus.type = ServerType.NEED_RESTORE;
          serverStatus.setterName = null;
        }
        return serverStatuses;
      } else {
        // caller is a server but not the master, master need s to change ONLINE server to be NEED_RESTORE server
        return serverStatuses;
      }
    }
  }

  ServerStatuses getServerStatuses() {
    ServerStatuses serverStatuses = new ServerStatuses();
    try {
      createNodeIfNotExist(getPath());
      // get all server statues
      for (String serverNameTemp : zookeeperClientRetry.getChildren(getPath())) {
        ServerStatus serverStatus = getServerStatus(serverNameTemp);
        if (serverStatus == null) continue;
        serverStatuses.add(serverStatus);
        log(Level.DEBUG, " find a server : ", serverStatus);
      }
    } catch (Throwable t) {
      log(Level.ERROR, " fail to get servers", t);
    }
    return serverStatuses;
  }

  ServerStatus getServerStatus(String serverName) {
    // get stat and data string
    Stat stat = new Stat();
    String dataString = null;
    try {
      dataString = new String(zookeeperClientRetry.getData(getPath() + "/" + serverName, stat));
    } catch (Throwable t) {
      log(Level.WARN, " fail to get data string for server name=", serverName, t);
      return null;
    }

    // parse data string to version and server type
    try {
      String[] stringArrayOfStatus = dataString.split(",", 3);
      return new ServerStatus(serverName, Long.valueOf(stringArrayOfStatus[0]), ServerType
          .valueOf(stringArrayOfStatus[1]), stringArrayOfStatus[2], stat.getCtime(), stat.getCzxid(), stat.getMtime());
    } catch (Throwable t) {
      log(Level.WARN, " fail to parse data string=", dataString, " for server name=", serverName, t);
      return null;
    }
  }

  synchronized public void register(String serverName, long dataVersion, ServerType serverType, String setterName)
      throws IOException {
    while (true) {
      try {
        createNodeIfNotExist(getPath());
        String serverPath = getPath() + "/" + serverName;
        String serverData = String.valueOf(dataVersion) + "," + serverType + "," + setterName;
        if (zookeeperClientRetry.exists(serverPath) == null) {
          zookeeperClientRetry.create(serverPath, serverData.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
          zookeeperClientRetry.setData(serverPath, serverData.getBytes());
        }
        return;
      } catch (Throwable t) {
        // this node is deleted by other client
        if (Utilities.getFirstCause(t) instanceof NoNodeException) continue;
        throw new IOException(t);
      }
    }
  }

  synchronized public void unregister(String serverName) throws IOException {
    createNodeIfNotExist(getPath());

    try {
      String serverPath = getPath() + "/" + serverName;
      Stat serverStat = zookeeperClientRetry.exists(serverPath);
      if (serverStat != null) {
        zookeeperClientRetry.delete(serverPath);
      }
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }

  Invocation invocationToGetStatusForLog = null;

  Object[] getStatus() {
    Object[] objects = new Object[getName() != null && !getName().isEmpty() ? 4 : 2];
    objects[0] = "address=";
    objects[1] = getAddress();
    if (getName() != null && !getName().isEmpty()) {
      objects[2] = "|name=";
      objects[3] = getName();
    }
    return objects;
  }

  void log(Level level, Object... objects) {
    Utilities.log(logger, level, invocationToGetStatusForLog, objects);
  }

  static public class ServerStatuses {
    private Map<String, ServerStatus> serverStatuses = new ConcurrentHashMap<String, ServerStatus>();

    public void add(ServerStatus serverStatus) {
      serverStatuses.put(serverStatus.name, serverStatus);
    }

    public ServerStatus electMaster(DistributedManager manager) {
      // try to elect a new master
      List<ServerStatus> serverStatusListForMasterElection =
          get(false, ServerType.MASTER, ServerType.SLAVE, ServerType.ONLINE);
      manager.log(Level.INFO, " serverStatusListForMasterElection=", serverStatusListForMasterElection,
          "|serverStatuses=", this);
      if (serverStatusListForMasterElection.isEmpty()) {
        manager.log(Level.INFO, " no server to elect a master");
        return null;
      }
      List<ServerStatus> serverStatusListWithMaxVersion =
          getServerStatusListWithMaxVersion(serverStatusListForMasterElection, manager);
      manager.log(Level.INFO, " serverStatusListWithMaxVersion=", serverStatusListWithMaxVersion);
      ServerStatus master = getFirstCreated(serverStatusListWithMaxVersion);

      // drop the new master if only one ONLINE server to elect and delay is not timeout
      long elapsedTime = System.currentTimeMillis() - master.createTime;
      if (serverStatusListForMasterElection.size() < manager.getElectionServersMin()
          && master.type.is(ServerType.ONLINE) && elapsedTime < manager.getElectionDelayTime()) {
        manager.log(Level.INFO, " ", master.name, " is elected as candidate master", "|elapsedTime=", elapsedTime,
            "|delayTime=", manager.getElectionDelayTime(), "|serverStatuses=", this);
        return null;
      }
      manager.log(Level.INFO, " ", master.name, " is elected as new master", "|elapsedTime=", elapsedTime,
          "|delayTime=", manager.getElectionDelayTime(), "|serverStatuses=", this);
      return master;
    }

    public void remove(String serverName) {
      serverStatuses.remove(serverName);
    }

    public boolean isEmpty() {
      return serverStatuses.isEmpty();
    }

    /**
     * is any serverType in serverTypes
     */
    public List<ServerStatus> get(boolean onlyTypeIsApplied, ServerType... serverTypes) {
      List<ServerStatus> serverStatusList = new ArrayList<ServerStatus>();
      for (ServerStatus serverStatus : serverStatuses.values()) {
        if (onlyTypeIsApplied && !serverStatus.isTypeApplied()) continue;
        for (ServerType serverType : serverTypes) {
          if (serverStatus.type.equals(serverType)) {
            serverStatusList.add(serverStatus);
            break;
          }
        }
      }
      return serverStatusList;
    }

    public List<ServerStatus> getNotStop(boolean onlyTypeIsApplied) {
      return getNot(onlyTypeIsApplied, ServerType.STOP);
    }

    /**
     * not all serverType in serverTypes
     */
    public List<ServerStatus> getNot(boolean onlyTypeIsApplied, ServerType... serverTypes) {
      List<ServerStatus> serverStatusList = new ArrayList<ServerStatus>();
      for (ServerStatus serverStatus : serverStatuses.values()) {
        if (onlyTypeIsApplied && !serverStatus.isTypeApplied()) continue;
        boolean condition = true;
        for (ServerType serverType : serverTypes) {
          if (serverStatus.type.equals(serverType)) {
            condition = false;
            break;
          }
        }
        if (condition) serverStatusList.add(serverStatus);
      }
      return serverStatusList;
    }

    public int size() {
      return serverStatuses.size();
    }

    public boolean isMaster(String serverName) {
      return serverName == null ? false : serverName.equals(getMaster(false));
    }

    public ServerStatus getMaster(boolean onlyTypeIsApplied) {
      List<ServerStatus> serverStatusList = get(onlyTypeIsApplied, ServerType.MASTER);
      return serverStatusList.isEmpty() ? null : serverStatusList.get(0);
    }

    public List<ServerStatus> getSlaves(boolean onlyTypeIsApplied) {
      return get(onlyTypeIsApplied, ServerType.SLAVE);
    }

    public List<ServerStatus> getMasterAndSlaves(boolean onlyTypeIsApplied) {
      return get(onlyTypeIsApplied, ServerType.MASTER, ServerType.SLAVE);
    }

    ServerStatus getFirstCreated(Collection<ServerStatus> serverStatusCollection) {
      if (serverStatusCollection == null) return null;
      ServerStatus firstCreatedServerStatus = null;
      for (ServerStatus serverStatus : serverStatusCollection) {
        if (firstCreatedServerStatus == null
            || firstCreatedServerStatus.createSerialNumber > serverStatus.createSerialNumber)
          firstCreatedServerStatus = serverStatus;
      }
      return firstCreatedServerStatus;
    }

    List<ServerStatus> get() {
      return new ArrayList<ServerStatus>(serverStatuses.values());
    }

    ServerStatus get(String serverName) {
      return serverStatuses.get(serverName);
    }

    public boolean isTypeApplied(String serverName) {
      ServerStatus serverStatus = get(serverName);
      if (serverStatus == null) return false;
      return serverStatus.isTypeApplied();
    }

    public Long getMaxVersion(List<ServerStatus> serverStatusList) {
      Long maxVersion = null;
      for (ServerStatus serverStatus : serverStatusList) {
        if (maxVersion == null || maxVersion < serverStatus.version) maxVersion = serverStatus.version;
      }
      return maxVersion;
    }

    public List<ServerStatus> getServerStatusListWithMaxVersion(List<ServerStatus> serverStatusListForGetMaxVersion,
        DistributedManager manager) {
      List<ServerStatus> serverStatusListWithMaxVersion = new ArrayList<ServerStatus>();
      Long maxVersion = getMaxVersion(serverStatusListForGetMaxVersion);
      manager.log(Level.INFO, " get max version=", maxVersion, " from serverStatusListForGetMaxVersion=",
          serverStatusListForGetMaxVersion);
      if (maxVersion == null) return serverStatusListWithMaxVersion;
      for (ServerStatus serverStatus : serverStatusListForGetMaxVersion) {
        if (serverStatus.version.equals(maxVersion)) serverStatusListWithMaxVersion.add(serverStatus);
      }
      return serverStatusListWithMaxVersion;
    }

    public String toString() {
      StringBuilder statusListString = new StringBuilder();
      statusListString.append("ServerStatus[").append(serverStatuses.size()).append(']').append("{");
      for (ServerType serverType : ServerType.values()) {
        for (ServerStatus serverStatus : ServerStatus.sortByTypeAndName(get(false, serverType))) {
          statusListString.append(serverStatus.toString()).append(',');
        }
      }
      if (serverStatuses.isEmpty()) statusListString.append("}");
      else statusListString.replace(statusListString.length() - 1, statusListString.length(), "}");
      return statusListString.toString();
    }

    /**
     * name|type|version|APPLIED,name|type|version|APPLYING,...
     */
    public String toStringWithNameVersionTypeApplied() {
      StringBuilder statusListString = new StringBuilder();
      for (ServerType serverType : ServerType.values()) {
        for (ServerStatus serverStatus : ServerStatus.sortByTypeAndName(get(false, serverType))) {
          statusListString.append(serverStatus.toStringWithNameVersionTypeProcess()).append(',');
        }
      }
      if (!serverStatuses.isEmpty()) statusListString.deleteCharAt(statusListString.length() - 1);
      return statusListString.toString();
    }
  }
}
