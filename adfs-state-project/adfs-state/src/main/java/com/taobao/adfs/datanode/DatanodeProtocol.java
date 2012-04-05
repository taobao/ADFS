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

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;

public interface DatanodeProtocol {
  static public final int lockId = 2;

  /**
   * Regist a datanode to datanode table, using the property values of layoutVersion, namespaceID, cTime, name,
   * storageID, infoPort, ipcPort to the dn table, other column in table init to 0, except the lastUpdate column, let it
   * as now() .
   * 
   * @throws IOException
   *           when some connect problem occurs
   * 
   * @return value: 0, success register, insert the datanode row in table; 1, the datanodeid is exists in datanode
   *         table, just update the other columns; 2, the regist failed, failed to insert or update the datanode table
   *         row
   */
  public int registerDatanodeBy(DatanodeRegistration registration, String hostName, long updateTime) throws IOException;

  /**
   * update the datanode relative properties to datanode table important: the lastUpdate time using the value of
   * parameter, not return value of now()
   * 
   * return true : if udpate success false: if udpate failed
   * 
   * @throws IOException
   *           when some connect problem occurs
   */
  public boolean handleHeartbeat(DatanodeRegistration dn, long capacity, long dfsUsed, long remaining,
      int xceiverCount, long updateTime, AdminStates adminState) throws IOException;

  /**
   * If the storageID already exists in datanode table? .
   * 
   * @throws IOException
   *           when some connect problem occurs
   */
  public boolean isDatanodeStorageIDExists(String storageId) throws IOException;

  /**
   * Get all DatanodeInfo whose value of (now() - lastUpdate query from dn table) < datanodeExpireInterval from datanode
   * table Please notice: the return class is DatanodeInfo, which contain capacity, dfsUsed, remaining, lastUpdate,
   * xceiverCount, location values .
   * 
   * @throws IOException
   *           when some connect problem occurs
   */
  public Collection<DatanodeInfo> getAliveDatanodes(long datanodeExpireInterval) throws IOException;
}
