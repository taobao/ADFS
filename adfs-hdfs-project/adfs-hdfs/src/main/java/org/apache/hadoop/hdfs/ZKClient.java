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

 package org.apache.hadoop.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.metrics.ZKMetrics;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 *  The zookeeper client for adfs to connect zookeeper server.
 *  Each has only one instance.
 */
public class ZKClient implements Watcher, Closeable {

  static final Log LOG = LogFactory.getLog(ZKClient.class);
  static final String SLASH = "/";
  static final String SEPARATOR = "-";

  private static String zkServer;
  static int sessionTimeOut; // msec
  static int maxRetryTimes;
  private static ZooKeeper zookeeper;
  private static Configuration conf;
  private static ZKClient _instance;
  static ZKMetrics metrics;

  // thread-safe singleton pattern
  public static ZKClient getInstance(Configuration conf_) {
    if (conf_ != null) {
      conf = conf_;
    }
    if (_instance == null) {
      synchronized (ZKClient.class) {
        if (_instance == null) {
          _instance = new ZKClient();
        }
      }
    }
    return _instance;
  }

  public static ZKClient getInstance() {
    return getInstance(null);
  }

  private ZKClient() {
    try {
      if (conf == null) {
        conf = new Configuration();
        conf.addResource("hdfs-default.xml");
        conf.addResource("hdfs-site.xml");
      }

      metrics = new ZKMetrics(conf);
      zkServer = conf.get("zookeeper.server.list");
      sessionTimeOut = conf.getInt("zookeeper.session.timeout", 180000);
      maxRetryTimes = conf.getInt("zookeeper.retry.max", 3);

      zookeeper = new ZooKeeper(zkServer, sessionTimeOut, this);
    } catch (IOException e) {
      LOG.fatal("******zookeeper client initialization failure******");
      throw new RuntimeException(e);
    }
    LOG.info("ZKClient is established.");
  }

  public final ZooKeeper getHandler() {
    return zookeeper;
  }

  /**
   * create a EPHEMERAL znode representing the path, create its parent
   * path if needed.
   * 
   * @param path
   * @param data
   * @param del if true, delete existed znode directly
   */
  public void touch(String path, byte[] data, boolean del) throws IOException {
    ZKUtil.touch(path, data, del);
  }

  /**
   * create a PERSISTENT znode according to input parameters
   * @param path the node you want to create
   * @param data the value associated with the node 
   * @param parent if it is true, it will check and create parent nodes;
   * @param sync if true, use the sync way
   * if false, it will create parent nodes only in case of NoNodeException
   */
  public String create(String path, byte[] data, boolean parent, boolean sync)
      throws IOException {
    metrics.numZKCreateOps.inc();
    if (sync) {
      return ZKUtil.syncCreate(path, data, parent);
    } else {
      ZKUtil.asyncCreate(path, data, parent);
      return path;
    }
  }

  /**
   * delete a znode representing the path with version number -1
   * @param path the path to be deleted in zookeeper
   * @param parent if it is true, delete its parent znode 
   * in case children number is 0
   * @param sync if true, use the sync way
   * @return
   * @throws IOException
   */
  public boolean delete(String path, boolean parent, boolean sync)
      throws IOException {
    metrics.numZKDeleteOps.inc();
    if (sync) {
      return ZKUtil.syncDelete(path, parent);
    } else {
      ZKUtil.asyncDelete(path, parent);
      return true;
    }
  }

  /**
   * delete a znode representing the path with version number -1
   * using async way
   * @param path the path to be deleted in zookeeper
   * @param parent if it is true, delete its parent znode 
   * in case children number is 0
   * @return void
   */
  public void delete(String path, boolean parent) {
    metrics.numZKDeleteOps.inc();
    ZKUtil.asyncDelete(path, parent);
  }

  /**
   * a sync way to check if a path exists
   * @param path path to check if it exists
   * @return Stat statuse of the path
   * @throws IOException
   */
  public Stat exist(String path) throws IOException {
    metrics.numZKExistOps.inc();
    return ZKUtil.syncExist(path);
  }

  public SimpleLock getSimpleLock(String lockhome, String lock) {
    return new SimpleLock(lockhome, lock);
  }

  /**
   * create a lock with the lock name as prefix in lock home 
   * the created znode representing the lock is an EPHEMERAL_SEQUENTIAL znode
   * @param lockhome
   * @param lock
   * @return the lock instance
   */
  public Lock getLock(String lockhome, String lock) throws IOException {
    return new Lock(lockhome, lock);
  }

  /**
   * create a read lock with the lock name as prefix in lock home
   * the created znode representing the lock is an EPHEMERAL_SEQUENTIAL znode
   * @param lockhome
   * @param lock
   * @return the lock instance
   */
  public ReadLock getReadLock(String lockhome, String lock) throws IOException {
    return new ReadLock(lockhome, lock);
  }

  /**
   * create a write lock with the lock name as prefix in lock home
   * the created znode representing the lock is an EPHEMERAL_SEQUENTIAL znode
   * @param lockhome
   * @param lock
   * @return the lock instance
   */
  public WriteLock getWriteLock(String lockhome, String lock)
      throws IOException {
    return new WriteLock(lockhome, lock);
  }

  /**
   * Set the data for the node of the given path if such a node exists 
   * and the given version matches the version of the node 
   * (if the given version is -1, it matches any node's versions). 
   * @param path the path of the node
   * @param data the data to set
   * @return the state of the node
   * @throws IOException
   */
  public void setData(String path, byte[] data) {
    metrics.numZKSetDataOps.inc();
    ZKUtil.asyncSetData(path, data);
  }

  /**
   * Return the data and the stat of the node of the given path. 
   * @param path the path of the node
   * @param stat the stat of the node, if null, get the latest
   * @return the data of the node
   * @throws IOException
   */
  public byte[] getData(String path, Stat stat) throws IOException {
    metrics.numZKGetDataOps.inc();
    return ZKUtil.syncGetData(path, stat);
  }

  /**
   * Return the list of the children of the node of the given path. 
   * @param path the path of the node
   * @param watcher explicit watcher
   * @return the data of the node
   * @throws IOException
   */
  public List<String> getChildren(String path, Watcher watcher)
      throws IOException {
    metrics.numZKGetChildrenOps.inc();
    return ZKUtil.syncGetChildren(path, watcher);
  }

  @Override
  public void process(WatchedEvent event) {
    LOG.debug("Received ZooKeeper Event, " + "type=" + event.getType() + ", "
        + "state=" + event.getState() + ", " + "path=" + event.getPath());
    switch (event.getType()) {
    // If event type is NONE, this is a connection status change
    case None: {
      break;
    }
    case NodeCreated: {
      break;
    }
    case NodeDeleted: {
      break;
    }
    case NodeDataChanged: {
      break;
    }
    case NodeChildrenChanged: {
      break;
    }
    }
  }

  /**
   * Close the zookeeper client. This can be only called in case the
   * namenode/datanode/client is shutting down
   */
  @Override
  public synchronized void close() throws IOException {
    try {
      zookeeper.close();
      _instance = null;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    throw new CloneNotSupportedException();
  }

  public static class SimpleLock {
    protected String lockhome; // user defined lock home
    protected String lock; // user defined prefix for lock
    protected String znode;

    protected SimpleLock(String lockhome, String lock) {
      this.lockhome = lockhome;
      this.lock = lock;
      this.znode = lockhome + SLASH + lock;
    }

    public boolean trylock() throws IOException {
      metrics.numZKSimpleLockOps.inc();
      return ZKUtil.syncCreate(znode);
    }

    public void unlock() throws IOException {
      metrics.numZKSimpleUnlockOps.inc();
      ZKUtil.asyncDelete(znode, false);
    }
  }

  public static class Lock implements Watcher {
    protected String lockhome; // user defined lock home
    protected String lock; // user defined prefix for lock
    protected String lockId; // generated lock id by zookeeper, usually lock +
                             // "0000xxx"
    protected String lastChildId; // the one I need to watch
    protected ZNodeName lockNode; // lock node, usually lockhome/lockId
    protected CountDownLatch signal; // replace the use of wait/notify;

    private static final Log LOG = LogFactory.getLog(Lock.class);

    protected Lock(String lockhome, String lock) throws IOException {
      this.lockhome = lockhome;
      this.lock = lock;
    }

    protected void init() throws IOException {
      try {
        long now = ZKClient.now();
        lockId = zookeeper.create(lockhome + SLASH + lock + SEPARATOR,
            new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        ZKClient.metrics.SyncWriteRate.inc((int) (ZKClient.now() - now));
        lockNode = new ZNodeName(lockId);
        lastChildId = null;
      } catch (KeeperException e) {
        throw new IOException(e);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }

    /**
     * lock operation returns immediately no matter it succeeded or not.
     * @return true, got the lock; otherwise false.
     * @throws IOException
     */
    public synchronized boolean trylock() throws IOException {
      return _lock(lock, false);
    }

    /**
     * lock operation with a time out value
     * 
     * @return true, got the lock; otherwise false
     * @throws IOException
     */
    public synchronized boolean lock() throws IOException {
      return _lock(lock, true);
    }

    protected boolean _lock(String filter, boolean wait) throws IOException {
      long start = ZKClient.now();
      do {
        try {
          if (ZKClient.now() - start > sessionTimeOut) {
            // failed to get the lock within a specified time period
            unlock();
            return false;
          }
          // check if the lockId is established.
          if (lockId == null) {
            init();
          }
          long now = ZKClient.now();
          List<String> children = zookeeper.getChildren(lockhome, false);
          ZKClient.metrics.ReadRate.inc((int) (ZKClient.now() - now));
          if (children.isEmpty()) {
            LOG.warn("FROM ZOOKEEPER (lock): lockhome: " + lockhome
                + " has no children");
            lockId = null;
            continue;
          } else {
            SortedSet<ZNodeName> sortedChildrenNodes = new TreeSet<ZNodeName>();
            for (String child : children) {
              if (child.startsWith(filter)) { // filter for specific locks
                sortedChildrenNodes
                    .add(new ZNodeName(lockhome + SLASH + child));
              }
            }
            if (sortedChildrenNodes.isEmpty()
                || (!sortedChildrenNodes.isEmpty() && lockId
                    .equals(sortedChildrenNodes.first().getName()))) {
              return true;
            }
            /*
             * add lockNode into sorted children node if not exists this is
             * useful when it is a read lock because its filter is for "WRITE"
             */
            if (!sortedChildrenNodes.contains(lockNode)) {
              sortedChildrenNodes.add(lockNode);
            }
            SortedSet<ZNodeName> lessThanMe = sortedChildrenNodes
                .headSet(lockNode);
            if (!lessThanMe.isEmpty()) {
              if (!wait) {
                // "try lock" doesn't need to wait to get the lock
                unlock();
                return false;
              }
              ZNodeName lastChildName = lessThanMe.last();
              lastChildId = lastChildName.getName();
              if (signal == null) {
                // reset CountDownLatch
                signal = new CountDownLatch(1);
              }
              now = ZKClient.now();
              Stat stat = zookeeper.exists(lastChildId, this);
              ZKClient.metrics.ReadRate.inc(ZKClient.now() - now);
              if (stat != null) {
                // wait for the event coming or time out
                long s = ZKClient.now();
                if (signal.await(sessionTimeOut, TimeUnit.MILLISECONDS)) {
                  // true if the count reached zero, which means it was notified
                  signal = null; // for reset
                }
                long delta = ZKClient.now() - s;
                LOG.info("ZOOKEEPER (lock) wait for " + lastChildId + " for "
                    + delta + " msec");
              }
              sortedChildrenNodes.clear();
              continue;
            } else {
              sortedChildrenNodes.clear();
              return true;
            }
          }
        } catch (KeeperException e) {
          throw new IOException(e);
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      } while (true);
    }

    public synchronized void unlock() {
      if (lockId != null) {
        try {
          long now = ZKClient.now();
          zookeeper.delete(lockId, -1);
          ZKClient.metrics.SyncWriteRate.inc((int) (ZKClient.now() - now));
        } catch (Exception e) {
          if (e instanceof KeeperException.NoNodeException) {
            LOG.warn("FROM ZOOKEEPER (lock): " + lockId + " doesn't exist.");
          } else {
            LOG.error("FROM ZOOKEEPER (lock): failed to unlock " + lockId + ","
                + "the error is: " + e);
          }
        } finally {
          lockId = null;
        }
      }
      ;
      lastChildId = null;
      lockNode = null;
    }

    public final String getLockId() {
      return lockId;
    }

    @Override
    public void process(WatchedEvent event) {
      // lets either become the leader or watch the new/updated node
      LOG.debug("FROM ZOOKEEPER (lock): Watcher fired on path: "
          + event.getPath() + " state: " + event.getState() + " type "
          + event.getType());
      signal.countDown();
    }
  }

  public static class WriteLock extends Lock {
    private static final String WRITE = "-WL-";

    protected WriteLock(String lockhome, String lock) throws IOException {
      super(lockhome, lock + WRITE);
    }

  }

  public static class ReadLock extends Lock {
    private static final String READ = "-RL-";
    private String wlock;

    protected ReadLock(String lockhome, String lock) throws IOException {
      super(lockhome, lock + READ);
      wlock = lock + WriteLock.WRITE;
    }

    public synchronized boolean trylock() throws IOException {
      return _lock(wlock, false);
    }

    public synchronized boolean lock() throws IOException {
      return _lock(wlock, true);
    }
  }

  /**
   * Represents an ephemeral znode name which has an ordered sequence number
   * and can be sorted in order (Excerpt from Zookeeper's recipes)
   */
  static class ZNodeName implements Comparable<ZNodeName> {
    private final String name;
    private String prefix;
    private int sequence = -1;
    private static final Log LOG = LogFactory.getLog(ZNodeName.class);

    public ZNodeName(String name) {
      if (name == null) {
        throw new NullPointerException("id cannot be null");
      }
      this.name = name;
      this.prefix = name;
      int idx = name.lastIndexOf(SEPARATOR);
      if (idx >= 0) {
        this.prefix = name.substring(0, idx);
        try {
          this.sequence = Integer.parseInt(name.substring(idx + 1));
          // If an exception occurred we misdetected a sequence suffix,
          // so return -1.
        } catch (NumberFormatException e) {
          LOG.error("Number format exception for " + idx, e);
        } catch (ArrayIndexOutOfBoundsException e) {
          LOG.error("Array out of bounds for " + idx, e);
        }
      }
    }

    @Override
    public String toString() {
      return name.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      ZNodeName sequence = (ZNodeName) o;

      if (!name.equals(sequence.name))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return name.hashCode() + 37;
    }

    public int compareTo(ZNodeName that) {
      int answer = this.prefix.compareTo(that.prefix);
      if (answer == 0) {
        int s1 = this.sequence;
        int s2 = that.sequence;
        if (s1 == -1 && s2 == -1) {
          return this.name.compareTo(that.name);
        }
        answer = s1 == -1 ? 1 : s2 == -1 ? -1 : s1 - s2;
      }
      return answer;
    }

    /**
     * Returns the name of the znode
     */
    public String getName() {
      return name;
    }

    /**
     * Returns the sequence number
     */
    public int getZNodeName() {
      return sequence;
    }

    /**
     * Returns the text prefix before the sequence number
     */
    public String getPrefix() {
      return prefix;
    }
  }

  static long now() {
    return System.currentTimeMillis();
  }
}
