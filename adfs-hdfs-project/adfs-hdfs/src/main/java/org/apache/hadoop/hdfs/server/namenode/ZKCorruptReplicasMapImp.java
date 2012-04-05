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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

public class ZKCorruptReplicasMapImp implements AbsCorruptReplicasMap, FSConstants {

  private static final Log LOG=LogFactory.getLog(ZKCorruptReplicasMapImp.class);
  private static final String SLASH = "/";
  
  public ZKCorruptReplicasMapImp() throws IOException{
    ZKClient.getInstance().create(FSConstants.ZOOKEEPER_CORRUPT_HOME, 
        new byte[0], false, true);
    LOG.info("ZKCorruptRelicasMapImp is initialized!");
  }

  @Override
  public boolean addToCorruptReplicasMap(Block blk, DatanodeDescriptor dn) throws IOException {
    StringBuilder sb=new StringBuilder(FSConstants.ZOOKEEPER_CORRUPT_HOME);
    sb.append(SLASH).append(blk.getBlockId());
    ZKClient.getInstance().create(sb.toString(), new byte[0], false, true);
    sb.append(SLASH).append(dn.getStorageID());
    String ret=ZKClient.getInstance().create(sb.toString(), 
        new byte[0], false, true);
    return ret!=null;
  }

  @Override
  public boolean removeFromCorruptReplicasMap(Block blk) throws IOException {
    StringBuilder sb=new StringBuilder(FSConstants.ZOOKEEPER_CORRUPT_HOME);
    sb.append(SLASH).append(blk.getBlockId());
    List<String> coll=ZKClient.getInstance().getChildren(sb.toString(), null);
    if(coll==null)
      return false;
    sb.append(SLASH);
    for(String one:coll){
      ZKClient.getInstance().delete(sb.toString()+one, true);
    }
    return true;
  }

  @Override
  public boolean removeFromCorruptReplicasMap(Block blk, DatanodeDescriptor dn) throws IOException {
    StringBuilder sb=new StringBuilder(FSConstants.ZOOKEEPER_CORRUPT_HOME);
    sb.append(SLASH).append(blk.getBlockId()).append(SLASH).append(dn.getStorageID());
    return ZKClient.getInstance().delete(sb.toString(), true, false);
  }

  @Override
  public boolean isReplicaCorrupt(Block blk, DatanodeDescriptor dn) throws IOException {
    StringBuilder sb=new StringBuilder(FSConstants.ZOOKEEPER_CORRUPT_HOME);
    sb.append(SLASH).append(blk.getBlockId()).append(SLASH).append(dn.getStorageID());
    Stat stat=ZKClient.getInstance().exist(sb.toString());
    return stat!=null;
  }
  
  @Override
  public boolean isReplicaCorrupt(Block blk) throws IOException {
    StringBuilder sb=new StringBuilder(FSConstants.ZOOKEEPER_CORRUPT_HOME);
    sb.append(SLASH).append(blk.getBlockId());
    Stat stat=ZKClient.getInstance().exist(sb.toString());
    return stat!=null;
  }

  @Override
  public int numCorruptReplicas(Block blk) throws IOException {
    StringBuilder sb=new StringBuilder(FSConstants.ZOOKEEPER_CORRUPT_HOME);
    sb.append(SLASH).append(blk.getBlockId());
    Stat stat = ZKClient.getInstance().exist(sb.toString());
    return (stat!=null?stat.getNumChildren():0);
  }
  
  @Override
  public List<String> getNodes(Block blk) throws IOException{
    StringBuilder sb=new StringBuilder(FSConstants.ZOOKEEPER_CORRUPT_HOME);
    sb.append(SLASH).append(blk.getBlockId());
    return ZKClient.getInstance().getChildren(sb.toString(), null);
  }

  @Override
  public int size() throws IOException {
    Stat stat = ZKClient.getInstance().exist(FSConstants.ZOOKEEPER_CORRUPT_HOME);
    return (stat!=null?stat.getNumChildren():0);
  }


}
