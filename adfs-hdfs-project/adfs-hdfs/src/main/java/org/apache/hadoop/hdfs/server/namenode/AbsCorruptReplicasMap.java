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

import org.apache.hadoop.hdfs.protocol.Block;

public interface AbsCorruptReplicasMap {
  
  boolean addToCorruptReplicasMap(Block blk,DatanodeDescriptor dn) throws IOException;
  
  boolean removeFromCorruptReplicasMap(Block blk) throws IOException;
  
  boolean removeFromCorruptReplicasMap(Block blk,DatanodeDescriptor dn) throws IOException;
  
  boolean isReplicaCorrupt(Block blk,DatanodeDescriptor dn) throws IOException;
  
  boolean isReplicaCorrupt(Block blk) throws IOException;
  
  int numCorruptReplicas(Block blk) throws IOException;
  
  int size() throws IOException;

  List<String> getNodes(Block blk) throws IOException;
}
