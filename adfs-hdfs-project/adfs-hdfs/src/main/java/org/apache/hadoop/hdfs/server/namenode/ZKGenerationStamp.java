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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.zookeeper.data.Stat;

/****************************************************************
 * A modified GenerationStamp class for adfs, it is implemented 
 * leveraging ZOOKEEPER, for one or more namenodes depends on it
 ****************************************************************/
public class ZKGenerationStamp implements FSConstants{
  
  public  static final long WILDCARD_STAMP = 1;
  public  static final long FIRST_VALID_STAMP = 1000L;
  private static final Charset CHARSET = Charset.forName("UTF-8");
  private static final int  BATCHSTAMP = 500;
  private static final Log LOG = LogFactory.getLog(ZKGenerationStamp.class);
  private final  AtomicLong catchedStamp = new AtomicLong(FIRST_VALID_STAMP);
  private final  AtomicLong avaliableStamp = new AtomicLong(0);
  private final  StringBuilder stampBuilder = new StringBuilder();
  private final  ReentrantReadWriteLock localLock = new ReentrantReadWriteLock();
  private final  ReadLock localReadLock = localLock.readLock();
  private final  WriteLock localWriteLock = localLock.writeLock();
  private final  DbLockManager lockManager;
  
  
  /**
   * Create a new instance, initialized to FIRST_VALID_STAMP.
   */
  public ZKGenerationStamp(DbLockManager locker) throws IOException {
    lockManager = locker;
    init();
  }
  
  private void init() throws IOException {
    ZKClient.getInstance().create(FSConstants.ZOOKEEPER_GENSTAMP_HOME, 
        new byte[0], false, true);
    LOG.info("ZKGenerationStamp initialized!");
  }

  /**
   * Returns the current generation stamp
   */
  public long getStamp() throws IOException {
    localReadLock.lock();
    try {
      return catchedStamp.get();
    } finally {
      localReadLock.unlock();
    }
  }
  
  public long nextStamp(long currentStamp) throws IOException {
    long val = FIRST_VALID_STAMP;
    localWriteLock.lock();
    try {
      if(catchedStamp.get() >= currentStamp) {
        return nextStamp();
      } else {
        /* we need to fetch a range of genstamps larger than currentStamp
         * , for this is called by nextGenerationStampForBlock
         * which requires a larger value than the current one */
        avaliableStamp.set(0);
        catchedStamp.set(val);
        return nextStamp();
      }
    }finally {
      localWriteLock.unlock();
    }
  }

  /**
   * First increments the counter and then returns the stamp 
   */
  public long nextStamp() throws IOException {
    long val = FIRST_VALID_STAMP;
    byte[] data = null;
    localWriteLock.lock();
    try {
      if(avaliableStamp.getAndDecrement() > 0) {
        return catchedStamp.incrementAndGet();
      } else { // newly or local cached stamp run-out
        if(lockManager.lockGenStamp()) {
          try{
            data = ZKClient.getInstance().getData(FSConstants.ZOOKEEPER_GENSTAMP_HOME, null);
            if(data != null) {
              String strGenStamp = new String(data, CHARSET);
              if(!strGenStamp.isEmpty()) {
                try {
                  val = Long.parseLong(strGenStamp);
                } catch (NumberFormatException nfe) {
                  LOG.error("cannot parse String:" + strGenStamp + " to Long", 
                      nfe);
                  throw nfe; //may exceeds the LONG.max, don't hold!
                }
              }
            }
            // we get such number stamps at one time
            catchedStamp.set(val+1);
            val += BATCHSTAMP;
            avaliableStamp.set(BATCHSTAMP-1);
            // flush new value to zookeeper
            stampBuilder.delete(0, stampBuilder.length());
            data = stampBuilder.append(val).toString().getBytes(CHARSET);
            ZKClient.getInstance().setData(FSConstants.ZOOKEEPER_GENSTAMP_HOME, data);
            return catchedStamp.get();
          } finally {
            lockManager.unlockGenStamp();
          }
        } else {
          throw new IOException("Cannot get write lock for genstamp");
        }
      }
    } finally {
      localWriteLock.unlock();
    }
  }
}
