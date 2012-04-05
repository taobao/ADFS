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

import org.apache.hadoop.hdfs.protocol.Block;

public class BlockInfo extends Block {
  /**
   * This array contains triplets of references. For each i-th data-node the
   * block belongs to triplets[i] is the reference to the DatanodeDescriptor
   */
  private DatanodeDescriptor[] datanode;

  public BlockInfo(Block blk, int replication) {
    super(blk);
    this.datanode = new DatanodeDescriptor[replication];
  }

  DatanodeDescriptor getDatanode(int index) {
    assert this.datanode != null : "BlockInfo is not initialized";
    assert index >= 0 && index < datanode.length : "Index is out of bound";
    DatanodeDescriptor node = (DatanodeDescriptor) datanode[index];
    assert node == null
        || DatanodeDescriptor.class.getName().equals(node.getClass().getName()) : "DatanodeDescriptor is expected at "
        + index;
    return node;
  }

  DatanodeDescriptor[] getDatanode() {
    assert this.datanode != null : "BlockInfo is not initialized";
    return this.datanode;
  }

  public void setDataNode(DatanodeDescriptor[] target) {
    assert this.datanode != null : "BlockInfo is not initialized";
    assert datanode.length == target.length : "Replication is not equal";
    for (int i = 0; i < target.length; i++) {
      datanode[i] = target[i];
    }
  }

  void setDatanode(int index, DatanodeDescriptor node) {
    assert this.datanode != null : "BlockInfo is not initialized";
    assert index >= 0 && index < datanode.length : "Index is out of bound";
    datanode[index] = node;
  }

  private int getCapacity() {
    assert this.datanode != null : "BlockInfo is not initialized";
    return datanode.length;
  }

  /**
   * Ensure that there is enough space to include num more triplets. * @return
   * first free triplet index.
   */
  private int ensureCapacity(int num) {
    assert this.datanode != null : "BlockInfo is not initialized";
    int last = numNodes();
    if (datanode.length >= (last + num))
      return last;
    /*
     * Not enough space left. Create a new array. Should normally happen only
     * when replication is manually increased by the user.
     */
    DatanodeDescriptor[] old = datanode;
    datanode = new DatanodeDescriptor[(last + num)];
    for (int i = 0; i < last; i++) {
      datanode[i] = old[i];
    }
    return last;
  }

  /**
   * Count the number of data-nodes the block belongs to.
   */
  int numNodes() {
    assert this.datanode != null : "BlockInfo is not initialized";
    for (int idx = getCapacity() - 1; idx >= 0; idx--) {
      if (getDatanode(idx) != null)
        return idx + 1;
    }
    return 0;
  }

  /**
   * Add data-node this block belongs to.
   */
  boolean addNode(DatanodeDescriptor node) {
    if (findDatanode(node) >= 0) // the node is already there
      return false;
    // find the last null node
    int lastNode = ensureCapacity(1);
    setDatanode(lastNode, node);
    return true;
  }

  /**
   * Remove data-node from the block.
   */
  boolean removeNode(DatanodeDescriptor node) {
    int dnIndex = findDatanode(node);
    if (dnIndex < 0) // the node is not found
      return false;
    // find the last not null node
    int lastNode = numNodes() - 1;
    // replace current node triplet by the lastNode one
    setDatanode(dnIndex, getDatanode(lastNode));
    // set the last triplet to null
    setDatanode(lastNode, null);
    return true;
  }

  /**
   * Find specified DatanodeDescriptor.
   * 
   * @param dn
   * @return index or -1 if not found.
   */
  int findDatanode(DatanodeDescriptor dn) {
    int len = getCapacity();
    for (int idx = 0; idx < len; idx++) {
      DatanodeDescriptor cur = getDatanode(idx);
      if (cur == dn)
        return idx;
      if (cur == null)
        break;
    }
    return -1;
  }
}