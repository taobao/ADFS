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

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.namenode.ZKUnderReplicatedBlocksImp.BlockIterator;

public interface AbsUnderReplicatedBlocks extends Iterable<Block>{
  
  static final int LEVEL = 3;
  
  boolean add(
      Block block,
      int curReplicas, 
      int decomissionedReplicas,
      int expectedReplicas) throws IOException;
  
  int getPriority(Block block, 
      int curReplicas, 
      int decommissionedReplicas,
      int expectedReplicas) throws IOException;
  
  boolean remove(Block block, 
      int oldReplicas, 
      int decommissionedReplicas,
      int oldExpectedReplicas) throws IOException;
  
  boolean remove(Block block, int priLevel) throws IOException;
  
  void update(Block block, int curReplicas, 
      int decommissionedReplicas,
      int curExpectedReplicas,
      int curReplicasDelta, int expectedReplicasDelta) throws IOException;
  
  boolean contains(Block block) throws IOException;
  
  int size();
  
  void clear();
  
  BlockIterator iterator();

}
