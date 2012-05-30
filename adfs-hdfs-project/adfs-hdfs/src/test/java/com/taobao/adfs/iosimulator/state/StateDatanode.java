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

package com.taobao.adfs.iosimulator.state;

import java.util.ArrayList;
import java.util.List;

public class StateDatanode extends StateTable{
  
  private static final String DATANODE = "datanode";
  public static final String[] columns = { "id", "name", "storageId", "ipcPort",
    "infoPort", "layoutVersion", "namespaceId", "ctime", "capacity",
    "dfsUsed", "remaining", "lastUpdated", "xceiverCount",
    "location", "hostName", "adminState", "version", "operateIdentifier"};
  
  private int id; // datanode id
  private String name;
  private String storageId;
  private int ipcPort;
  private int infoPort;
  private int layoutVersion;
  private int namespaceId;
  private long ctime;
  private long capactiy;
  private long dfsUsed;
  private long remaining;
  private long lastUpdated;
  private int xceiverCount;
  private String location;
  private String hostName;
  private short adminState;
  
  public static StateDatanode createDatanode(int id) {
    return new StateDatanode(id);
  }
  
  private StateDatanode(int id) {
    this.id = id;
    name = DATANODE + id;
    storageId = DATANODE + id;
    lastUpdated = System.currentTimeMillis();
    location = DATANODE + id;
    hostName = DATANODE + id;
    version = id;
  }
  
  @Override
  public String[] getFields(ExecuteType type) {
    switch(type) {
    case CREATEDATANODE:
      return columns;
    case UPDATEDATANODE:
      return new String[] {String.valueOf(id)};
    case DELETEDATANODE:
      break;
    }
    return null;
  }

  @Override
  public String[] toExecutable(ExecuteType type) {
    switch(type) {
    case CREATEDATANODE:
      return strarrayCreateDatanode();
    case UPDATEDATANODE:
      return strarrayUpdateDatanode();
    case DELETEDATANODE:
    }
    return null;
  }
  
  private String[] strarrayCreateDatanode() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(id));
    args.add(name);
    args.add(storageId);
    args.add(String.valueOf(ipcPort));
    args.add(String.valueOf(infoPort));
    args.add(String.valueOf(layoutVersion));
    args.add(String.valueOf(namespaceId));
    args.add(String.valueOf(ctime));
    args.add(String.valueOf(capactiy));
    args.add(String.valueOf(dfsUsed));
    args.add(String.valueOf(remaining));
    args.add(String.valueOf(lastUpdated));
    args.add(String.valueOf(xceiverCount));
    args.add(location);
    args.add(hostName);
    args.add(String.valueOf(adminState));
    args.add(String.valueOf(version));
    args.add(operateIdentifier);
    return args.toArray(new String[0]);
  }
  
  private String[] strarrayUpdateDatanode() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(id));
    args.add(name);
    args.add(storageId);
    args.add(String.valueOf(ipcPort));
    args.add(String.valueOf(infoPort));
    args.add(String.valueOf(layoutVersion));
    args.add(String.valueOf(namespaceId));
    args.add(String.valueOf(ctime));
    args.add(String.valueOf(capactiy));
    args.add(String.valueOf(dfsUsed));
    args.add(String.valueOf(remaining));
    args.add(String.valueOf(lastUpdated));
    args.add(String.valueOf(xceiverCount));
    args.add(location);
    args.add(hostName);
    args.add(String.valueOf(adminState));
    args.add(String.valueOf(version));
    args.add(operateIdentifier);
    return args.toArray(new String[0]);
  }

  @Override
  public String[] getKeys(ExecuteType type) {
    // TODO Auto-generated method stub
    return null;
  }

}
