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
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.Daemon;

import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.file.File;
import com.taobao.adfs.state.StateManager;

public class TrashCleaner {
  private static final Log LOG = LogFactory.getLog(TrashCleaner.class);
  private final FSNamesystem namesystem;
  private final StateManager stateManager;
  private final static String trashHome = FSConstants.ZOOKEEPER_TRASH_HOME;
  private final static String trashToken = "token";
  private Daemon worker;
  
  TrashCleaner(FSNamesystem namesystem) throws IOException {
    this.namesystem = namesystem;
    this.stateManager = namesystem.stateManager;
    ZKClient.getInstance().create(trashHome, new byte[0], false, true);
    worker = new Daemon(new Worker());
    worker.start();
    LOG.info("TrashCleaner initialized");
  }
  
  private class Worker implements Runnable {
    @Override
    public void run() {
      ZKClient.SimpleLock lock = 
        ZKClient.getInstance().getSimpleLock(trashHome, trashToken);
      while(!Thread.currentThread().isInterrupted()) {
        try {
          if(lock.trylock()) {
            try {
                clean();
            } finally {
              lock.unlock();
            }
          }      
        } catch (Exception e) {
          LOG.error("TrashCleaner got Exception", e);
        }
        try {
          TimeUnit.MINUTES.sleep(1L);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    
    private void clean() throws IOException {
      String trashPath = StateManager.getTrashPath();
      File[] files = stateManager.getListing(trashPath);
      if(files != null) {
        List<BlockEntry> belist = stateManager.getBlocksByFiles(files);
        Block b = new Block();
        for (BlockEntry be : belist) {
          b.set(be.getBlockId(), be.getNumbytes(), be.getGenerationStamp());
          namesystem.corruptReplicas.removeFromCorruptReplicasMap(b);
          for (Integer dnid : be.getDatanodeIds()) {
            DatanodeDescriptor dn = namesystem.getDataNodeDescriptorByID(dnid);
            if(dn != null) namesystem.addToInvalidates(b, dn);
          }
          stateManager.removeBlock(b.getBlockId());
        }
        // we clean the files at last.
        stateManager.delete(files, true);
      }
    } // end of clean
  }
  
  void close() {
    worker.interrupt();
    try {
      worker.join(1000); // sleep for 1K msec
    } catch (InterruptedException ignored) {
    }
  }
}
