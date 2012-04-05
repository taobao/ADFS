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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.taobao.adfs.state.StateManager;

public class DbLockManager {

  private final StateManager state;
  // state's expire period
  private volatile long stateLockExpirePeriod;
  // state's timeout period
  private volatile long stateLockTimeOutPeriod;
  // random attribute
  private static Random r = new Random();
  
  // state lock types
  enum StateLockType {
    FILE,  // ordinal == 0
    BLOCK, // ordinal == 1
    DATANODE, // ordinal == 2
    REPMONITOR, // ordinal == 3
    LEASEMONITOR, // ordinal == 4
    PENDINGMONITOR, // ordinal == 5
    INVALIDATEMONITOR, // ordinal == 6
    GENSTAMP // ordinal == 7
  }
  
  DbLockManager(StateManager state, long expire, long timeout) {
    this.state = state;
    stateLockExpirePeriod = expire;
    stateLockTimeOutPeriod = timeout;
  }
  
  public boolean lockFile(int fileId) throws IOException {
    return state.lock(stateLockExpirePeriod, stateLockTimeOutPeriod, 
        StateLockType.FILE.ordinal(), fileId);
  }
  
  public boolean unlockFile(int fileId) throws IOException {
    return state.unlock(StateLockType.FILE.ordinal(), fileId);
  }
  
  public boolean lockBlock(long blockId) throws IOException {
    return state.lock(stateLockExpirePeriod, stateLockTimeOutPeriod, 
        StateLockType.BLOCK.ordinal(), blockId);
  }
  
  
  private static long now() {
    return System.currentTimeMillis();
  }
  
  // first acquire file lock and then acquire block lock
  public boolean lockFileBlock(int fileId, long blockId) throws IOException {
    boolean redo = false, ret = true;
    long firstFileLockAcquiredTime = Long.MAX_VALUE;
    int base = 50; // at most sleep time
    
    while(true) {
      if(!state.tryLock(stateLockExpirePeriod, 
          StateLockType.FILE.ordinal(), fileId)) {
        try {
          TimeUnit.MILLISECONDS.sleep(
              r.nextInt(base));
        } catch (InterruptedException ignored) { }
      } else {
        // acquired file lock
        firstFileLockAcquiredTime = now();
        while(true) {
          boolean locked = false; // is block locked?
          if(!(locked = state.tryLock(stateLockExpirePeriod, 
              StateLockType.BLOCK.ordinal(), blockId))) {
            try {
              TimeUnit.MILLISECONDS.sleep(
                  r.nextInt(base));
            } catch (InterruptedException ignored) { }
          }
          
          if(now()-firstFileLockAcquiredTime 
              > stateLockExpirePeriod - 100*base) {
            // the file lock is about to expire
            // reset and try again
            if(locked) {
              unlockBlock(blockId);
            }
            unlockFile(fileId);
            firstFileLockAcquiredTime = Long.MAX_VALUE;
            redo = true;
            break;
          } else if (locked) {
            break;
          }
        } // end inner while
        
        if(redo) {
          redo = false;
          continue;
        } else {
          break;
        }
      }
    } // end outer while
    
    return ret;
  }
  
  // first unlock block lock and then unlock file lock
  public boolean unlockFileBlock(int fileId, long blockId) throws IOException {
    boolean ret = true;
    try {
      ret &= unlockBlock(blockId);
    } finally {
      ret &= unlockFile(fileId);
    }
    return ret;
  }
  
  // a temporary approach to acquire all locks of one file's blocks
  // which may be very time-consuming
  public boolean lockBlocks(long[] blockIds) throws IOException {
    boolean redo = false, ret = true;
    long firstBlockLockAcquiredTime = Long.MAX_VALUE;
    int base = 50; // at most sleep time

    if (blockIds != null) {
      int len = blockIds.length, i;
      for(i = 0; i < len; i++) {
        while(true) {
          if(now()-firstBlockLockAcquiredTime > stateLockExpirePeriod - 100*base) {
            // we need to secure that there is enough time buffer for rest ops
            redo = true;
            break;
          }
          if(!state.tryLock(stateLockExpirePeriod, 
              StateLockType.BLOCK.ordinal(), blockIds[i])) {
            try {
              TimeUnit.MILLISECONDS.sleep(r.nextInt(base));
            } catch (InterruptedException ignored) { }
          } else {
            break; // jump out the loop
          }
        }
        if(i == 0 && !redo) {
          firstBlockLockAcquiredTime = now();
        } else if (redo) {
          if(i > 0) { // release previous lock if any
            long[] newblkIds = new long[i];
            System.arraycopy(blockIds, 0, newblkIds, 0, i);
            unlockBlocks(newblkIds);
          }
          firstBlockLockAcquiredTime = Long.MAX_VALUE;
          redo = false;
          i=-1; // reset to -1, let it ++1
        }
      }
    }
    
    return ret;
  }
  
  public boolean unlockBlock(long blockId) throws IOException {
    return state.unlock(StateLockType.BLOCK.ordinal(), blockId);
  }
  
  // all unlockBlocks success return true
  public boolean unlockBlocks(long[] blockIds) throws IOException {
    boolean ret = true;
    if(blockIds != null) {
      int len = blockIds.length;
      for(int i = 0; i < len; i++) {
        if(!unlockBlock(blockIds[i])) {
          ret = false;
        }
      }
    }
    return ret;
  }
  
  public boolean trylockDataNode(int danodeId) throws IOException {
    return state.tryLock(stateLockExpirePeriod, 
        StateLockType.DATANODE.ordinal(), danodeId);
  }
  
  public boolean unlockDataNode(int danodeId) throws IOException {
    return state.unlock(StateLockType.DATANODE.ordinal(), danodeId);
  }
  
  public boolean trylockRepMonitor() throws IOException {
    return state.tryLock(stateLockExpirePeriod, 
        StateLockType.REPMONITOR.ordinal(), 
        StateLockType.REPMONITOR.ordinal());
  }
  
  public boolean unlockRepMonitor() throws IOException {
    return state.unlock(StateLockType.REPMONITOR.ordinal(), 
        StateLockType.REPMONITOR.ordinal());
  }
  
  public boolean trylockLeaseMonitor() throws IOException {
    return state.tryLock(stateLockExpirePeriod, 
        StateLockType.LEASEMONITOR.ordinal(), 
        StateLockType.LEASEMONITOR.ordinal());
  }
  
  public boolean unlockLeaseMonitor() throws IOException {
    return state.unlock(StateLockType.LEASEMONITOR.ordinal(), 
        StateLockType.LEASEMONITOR.ordinal());
  }
  
  public boolean trylockPendingMonitor() throws IOException {
    return state.tryLock(stateLockExpirePeriod,  
        StateLockType.PENDINGMONITOR.ordinal(), 
        StateLockType.PENDINGMONITOR.ordinal());
  }
  
  public boolean unlockPendingMonitor() throws IOException {
    return state.unlock(StateLockType.PENDINGMONITOR.ordinal(), 
        StateLockType.PENDINGMONITOR.ordinal());
  }
  
  public boolean trylockInvalidateMonitor() throws IOException {
    return state.tryLock(stateLockExpirePeriod, 
        StateLockType.INVALIDATEMONITOR.ordinal(), 
        StateLockType.INVALIDATEMONITOR.ordinal());
  }
  
  public boolean unlockInvalidateMonitor() throws IOException {
    return state.unlock(StateLockType.INVALIDATEMONITOR.ordinal(), 
        StateLockType.INVALIDATEMONITOR.ordinal());
  }
  
  public boolean lockGenStamp() throws IOException {
    return state.lock(stateLockExpirePeriod, stateLockTimeOutPeriod, 
        StateLockType.GENSTAMP.ordinal(), 
        StateLockType.GENSTAMP.ordinal());
  }
  
  public boolean unlockGenStamp() throws IOException {
    return state.unlock(StateLockType.GENSTAMP.ordinal(), 
        StateLockType.GENSTAMP.ordinal());
  }
  
}
