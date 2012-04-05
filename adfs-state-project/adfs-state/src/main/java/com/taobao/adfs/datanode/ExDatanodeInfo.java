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

package com.taobao.adfs.datanode;

import org.apache.hadoop.hdfs.protocol.*;

/**
 * @author <a href=mailto:jushi@taobao.com>jushi</a>
 */
public final class ExDatanodeInfo extends DatanodeInfo {
  public ExDatanodeInfo(Datanode datanode) {
    this.name = datanode.name;
    this.storageID = datanode.storageId;
    this.infoPort = datanode.infoPort;
    this.ipcPort = datanode.ipcPort;
    this.capacity = datanode.capacity;
    this.dfsUsed = datanode.dfsUsed;
    this.remaining = datanode.remaining;
    this.lastUpdate = datanode.lastUpdated;
    this.xceiverCount = datanode.xceiverCount;
    this.location = datanode.location;
    this.hostName = datanode.hostName;
    this.adminState = getAdminState(datanode.adminState);
  }

  public AdminStates getAdminState2() {
    if (adminState == null) { return AdminStates.NORMAL; }
    return adminState;
  }

  static public AdminStates getAdminState(int adminStateCode) {
    switch (adminStateCode) {
    case 0:
      return AdminStates.NORMAL;
    case 1:
      return AdminStates.DECOMMISSION_INPROGRESS;
    case 2:
    default:
      return AdminStates.DECOMMISSIONED;
    }
  }

  static public byte getAdminState(AdminStates adminState) {
    switch (adminState) {
    case NORMAL:
      return 0;
    case DECOMMISSION_INPROGRESS:
      return 1;
    case DECOMMISSIONED:
    default:
      return 2;
    }
  }
}
