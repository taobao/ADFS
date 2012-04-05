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

 package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.zookeeper.data.Stat;

public class ZKExcessReplicationMapImp implements AbsExcessReplicationMap, FSConstants{

  private static final Log LOG = LogFactory.getLog(ZKExcessReplicationMapImp.class);
  private static final String SLASH = "/";
  
  public ZKExcessReplicationMapImp() throws IOException {
    ZKClient.getInstance().create(FSConstants.ZOOKEEPER_EXCESS_HOME, 
        new byte[0], false, true);
    LOG.info("ZKExcessReplicationMapImp is initialized!");
  }

  @Override
  public boolean exist(String storageID, Block block) throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_EXCESS_HOME);
    sb.append(SLASH).append(storageID).append(SLASH).append(block.getBlockId());
    Stat stat = null;
    stat = ZKClient.getInstance().exist(sb.toString());
    return (stat!=null);
  }


  @Override
  public boolean put(String storageID, Block block) throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_EXCESS_HOME);
    sb.append(SLASH).append(storageID);
    ZKClient.getInstance().create(sb.toString(), new byte[0], false, true);
    sb.append(SLASH).append(block.getBlockId());
    String ret = null;
    ret = ZKClient.getInstance().create(sb.toString(), 
        block.toString().getBytes(), false, true);
    return (ret!=null);
  }


  @Override
  public boolean remove(String storageID, Block block) throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_EXCESS_HOME);
    sb.append(SLASH).append(storageID).append(SLASH).append(block.getBlockId());
    boolean ret = false;
    ret = ZKClient.getInstance().delete(sb.toString(), true, true);
    return ret;
  }

  @Override
  public Collection<String> get(String storageID) throws IOException {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_EXCESS_HOME);
    sb.append(SLASH).append(storageID);
    return ZKClient.getInstance().getChildren(sb.toString(), null);
  }
}
