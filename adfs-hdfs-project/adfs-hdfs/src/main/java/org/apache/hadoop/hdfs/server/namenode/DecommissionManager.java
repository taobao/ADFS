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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Manage node decommissioning.
 */
class DecommissionManager {
  static final Log LOG = LogFactory.getLog(DecommissionManager.class);

  private final FSNamesystem fsnamesystem;

  DecommissionManager(FSNamesystem namesystem) {
    this.fsnamesystem = namesystem;
  }

  /** Periodically check decommission status. */
  class Monitor implements Runnable {
    /**
     * recheckInterval is how often namenode checks
     * if a node has finished decommission
     */
    private final long recheckInterval;
    /** The number of decommission nodes to check for each interval */
    private final int numNodesPerCheck;
    /** firstkey can be initialized to anything. */
    private String firstkey = "";

    Monitor(int recheckIntervalInSecond, int numNodesPerCheck) {
      this.recheckInterval = recheckIntervalInSecond * 1000L;
      this.numNodesPerCheck = numNodesPerCheck;
    }

    /**
     * Check decommission status of numNodesPerCheck nodes
     * for every recheckInterval milliseconds.
     */
    public void run() {
      for (; fsnamesystem.isRunning();) {
        try {
          Thread.sleep(recheckInterval);
          FSNamesystem.getFSNamesystem().namenode.getClient().decommisionCheck();
        } catch (Throwable t) {
          LOG.info("exception: " + this.getClass().getSimpleName(), t);
        }
      }
    }

    public void check() throws IOException {
      int count = 0;
      List<DatanodeDescriptor> nodeList = FSNamesystem.getFSStateManager().getDatanodeDescriptorList(false);
      for (DatanodeDescriptor node : nodeList) {
        if (firstkey == null || firstkey.equals(node.getStorageID())) {
          if (count == numNodesPerCheck) {
            firstkey = node.getStorageID();
            return;
          }
          if (node.isDecommissionInProgress()) {
            try {
              fsnamesystem.checkDecommissionStateInternal(node);
            } catch (Exception e) {
              LOG.warn("node=" + node, e);
            }
          }
          ++count;
        } else continue;
      }
      firstkey = null;
    }
  }
}