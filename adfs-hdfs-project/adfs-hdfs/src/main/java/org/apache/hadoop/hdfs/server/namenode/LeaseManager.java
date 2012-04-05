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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.WeakHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.ZKClient;

/**
 * LeaseManager does the lease housekeeping for writing on files.   
 * This class also provides useful static methods for lease recovery.
 * 
 * Lease Recovery Algorithm
 * 1) Namenode retrieves lease information
 * 2) For each file f in the lease, consider the last block b of f
 * 2.1) Get the datanodes which contains b
 * 2.2) Assign one of the datanodes as the primary datanode p

 * 2.3) p obtains a new generation stamp form the namenode
 * 2.4) p get the block info from each datanode
 * 2.5) p computes the minimum block length
 * 2.6) p updates the datanodes, which have a valid generation stamp,
 *      with the new generation stamp and the minimum block length 
 * 2.7) p acknowledges the namenode the update results

 * 2.8) Namenode updates the BlockInfo
 * 2.9) Namenode removes f from the lease
 *      and removes the lease once all files have been removed
 * 2.10) Namenode commit changes to edit log
 */
public class LeaseManager {
  public static final Log LOG = LogFactory.getLog(LeaseManager.class);
  private static final String MONITOR_LOCK_PREFIX = "lockm-";
  private static final Charset CHARSET = Charset.forName("UTF-8");
  private static final String SLASH = "/";
    
  private final FSNamesystem fsnamesystem;
  private final DbLockManager lockManager;
  private long softLimit = FSConstants.LEASE_SOFTLIMIT_PERIOD;
  private long hardLimit = FSConstants.LEASE_HARDLIMIT_PERIOD;
  
  LeaseManager(FSNamesystem fsnamesystem) throws IOException {
    this.fsnamesystem = fsnamesystem;
    this.lockManager = fsnamesystem.lockManager;
    ZKClient.getInstance().create(FSConstants.ZOOKEEPER_LEASE_HOME, 
        new byte[0], false, true);
  }
  
  Lease getLease(String holder, boolean loadLeaseFiles) throws IOException {
    Lease lease = null;
    if(holder != null) {
      lease = globalGetLeaseInfo(holder, loadLeaseFiles);
    }
    return lease;
  }

  /** @return the lease containing src 
   * @throws IOException */
  public Lease getLeaseById(int fileId) throws IOException {
    Lease lease = null;
    String holder = globalFindHolder(fileId);
    if(holder != null) {
      lease = getLease(holder, true);
    }
    return lease;
  }

  /** @return the number of paths contained in all leases 
   * @throws IOException */
  int countPath() throws IOException {
    int count = 0;
    List<String> holders = globalGetLeaseClient();
    if(holders != null) {
      for(String holder : holders) {
        List<String> files = globalGetLeaseFiles(holder);
        if(files != null) {
          count += files.size();
        }
      }
    }
    return count;
  }
  
  /**
   * Adds (or re-adds) the lease for the specified file.
   * @throws IOException 
   */
  Lease addLease(String holder, int fileId) throws IOException {
    Lease lease = null;
    lease = globalAddLease(holder, fileId);
    return lease;
  }

  /**
   * Reassign lease for file src to the new holder.
   * @throws IOException 
   */
  Lease reassignLease(Lease lease, int fileId, 
      String newHolder) throws IOException {
    assert newHolder != null : "new lease holder is null";
    if (lease != null) {
      removeLease(lease, fileId);
    }
    return addLease(newHolder, fileId);
  }
  
  /**
   * Remove the specified lease and src.
   * @throws IOException 
   */
  void removeLease(Lease lease, int fileId) throws IOException {
    String holder = lease.holder;
    removeLease(holder, fileId);
  }

  /**
   * Remove the lease for the specified holder and src
   * @throws IOException 
   */
  void removeLease(String holder, int fileId) throws IOException {
     globalRemoveLeaseFile(holder, fileId);
  }
  
  /**
   * Remove the lease for the specified src
   * @param fileId
   * @throws IOException
   */
  void removeLease(int fileId) throws IOException {
    String holder = globalFindHolder(fileId);
    if(holder != null) {
      removeLease(holder, fileId);
    }
  }
  
  /**
   * Remove the lease for the specified holder with all files
   * @throws IOException 
   */
  void removeLeases(String holder, List<String> files) throws IOException {
      globalRemoveLeaseFiles(holder, files);
  }
  
  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder) throws IOException {
    renewLease(getLease(holder, false));
  }
  
  void renewLease(Lease lease) throws IOException {
    if (lease != null) {
      lease.renew();
      globalUpdateLeaseInfo((ZkLeaseInfo) lease);
    }
  }

  /************************************************************
   * A Lease governs all the locks held by a single client.
   * For each client there's a corresponding lease, whose
   * timestamp is updated when the client periodically
   * checks in.  If the client dies and allows its lease to
   * expire, all the corresponding locks can be released.
   *************************************************************/
  class Lease implements Comparable<Lease> {
    protected String holder;
    protected long lastUpdate;
    protected Collection<String> fileIds = new TreeSet<String>();
  
    /** Only LeaseManager object can create a lease */
    private Lease(String holder) {
      this.holder = holder;
      renew();
    }
    /** Only LeaseManager object can renew a lease */
    protected void renew() {
      this.lastUpdate = NameNode.now();
    }

    /** @return true if the Hard Limit Timer has expired */
    public boolean expiredHardLimit() {
      return NameNode.now() - lastUpdate > hardLimit;
    }

    /** @return true if the Soft Limit Timer has expired */
    public boolean expiredSoftLimit() {
      return NameNode.now() - lastUpdate > softLimit;
    }
    
    public String getHolder() {
      return holder;
    }

    /**
     * @return the path associated with the pendingFile and null if not found.
     */
    private String findFileId(int fileId) {
      String strFileId = Integer.toString(fileId);
      if(fileIds.contains(strFileId)) {
        return strFileId;
      } else {
        return null;
      }
    }

    /** Does this lease contain any path? */
    boolean hasFiles() {return !fileIds.isEmpty();}

    /** {@inheritDoc} */
    public String toString() {
      return "[Lease.  Holder: " + holder
          + ", pendingcreates: " + fileIds + "]";
    }
  
    /** {@inheritDoc} */
    public int compareTo(Lease o) {
      Lease l1 = this;
      Lease l2 = o;
      long lu1 = l1.lastUpdate;
      long lu2 = l2.lastUpdate;
      if (lu1 < lu2) {
        return -1;
      } else if (lu1 > lu2) {
        return 1;
      } else {
        return l1.holder.compareTo(l2.holder);
      }
    }
  
    /** {@inheritDoc} */
    public boolean equals(Object o) {
      if (!(o instanceof Lease)) {
        return false;
      }
      Lease obj = (Lease) o;
      if (lastUpdate == obj.lastUpdate &&
          holder.equals(obj.holder)) {
        return true;
      }
      return false;
    }
  
    /** {@inheritDoc} */
    public int hashCode() {
      return holder.hashCode();
    }
    
    Collection<String> getFiles() {
      return fileIds;
    }
  }

  public void setLeasePeriod(long softLimit, long hardLimit) {
    this.softLimit = softLimit;
    this.hardLimit = hardLimit; 
  }
  
  /******************************************************
   * Monitor checks for leases that have expired,
   * and disposes of them.
   ******************************************************/
  class Monitor implements Runnable {
    final String name = getClass().getSimpleName();

    
    /** Check leases periodically. */
    public void run() {
      for (; fsnamesystem.isRunning();) {
        try {
          if (lockManager.trylockLeaseMonitor()) {
            try {
              checkLeases();
            } catch(IOException ioe) {
              LOG.error("LeaseManager's monitor IO exception", ioe);
            } finally {
              lockManager.unlockLeaseMonitor();
            }
          }
        } catch (Exception e) {
          LOG.error("Lease Manager Monitor got exception...", e);
        }
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(name + " is interrupted", ie);
          }
        }
      }
    }
  }

  /** Check the leases beginning from the oldest. 
   * @throws IOException */
  void checkLeases() throws IOException {
    SortedSet<Lease> sortedLeases;
    while((sortedLeases = globalGetLeases()).size() > 0) {
      final Lease oldest = sortedLeases.first();
      if (!oldest.expiredHardLimit()) {
        return;
      }

      LOG.info("Lease " + oldest + " has expired hard limit");

      final List<String> removing = new ArrayList<String>();
      /* cause we get collection remotely and don't need to 
       * worry about ConcurrentModifyException*/
      Collection<String> oldestFiles = oldest.getFiles();
      if(oldestFiles != null) {
        for(String p : oldestFiles) {
          try {
            fsnamesystem.internalReleaseLeaseOne(oldest, Integer.parseInt(p));
          } catch (IOException e) {
            LOG.error("Cannot release the path "+p+" in the lease "+oldest, e);
            removing.add(p);
          }
        }
      }


      // remove the client node and all its files
      removeLeases(oldest.holder, removing);
    }
  }

  /** {@inheritDoc} */
  public String toString() {
    return getClass().getSimpleName();
  }
  
  /* ----- Global related operations on ZooKeeper  ----- */
  
  /**
   * return the namenode address requesting the lease
   */
  public ZkLeaseInfo globalGetLeaseInfo(String holder, boolean loadLeaseFiles) throws IOException {
    String node = buildLeaseInfoPath(holder);
    byte[] data = ZKClient.getInstance().getData(node, null);
    if(data == null) {
      return null; // doesn't exist
    } else {
      ZkLeaseInfo li = new ZkLeaseInfo(data, loadLeaseFiles);
      return li;
    }
  }
  
  private Lease globalAddLease(String holder, int fileId) throws IOException {
    String node = buildLeaseInfoPath(holder);
    byte[] data = ZKClient.getInstance().getData(node, null);
    ZkLeaseInfo li = null;
    if(data == null) {
      // doesn't exist lease info node, then create
      li = new ZkLeaseInfo(holder);
      ZKClient.getInstance().create(node, li.toByteArray(), false, false);
    } else {
      // existed, then update lastUpdate value, don't load lease files
      li = new ZkLeaseInfo(data, false); // super will renew lastUpdate value
      ZKClient.getInstance().setData(node, li.toByteArray());
    }
    // now, we can add the lease file node
    globalAddLeaseFile(holder, fileId);
    // It's safe not to load lease files here for performance boost
    // li.loadLeaseFiles();
    return li;
  }
  
  private void globalRemoveLeaseFile(String holder, int fileId) throws IOException {
    String node = buildLeaseFilePath(holder, fileId);
    ZKClient.getInstance().delete(node, true);
  }
  
  private void globalRemoveLeaseFiles(String holder, List<String> files) throws IOException {
    String node = buildLeaseInfoPath(holder);
    if(files != null) {
      StringBuilder sb = new StringBuilder();
      for(String file : files) {
        sb.delete(0, sb.length());
        sb.append(node).append(SLASH).append(file);
        ZKClient.getInstance().delete(sb.toString(), true);
      }
    }
    ZKClient.getInstance().delete(node, false);
  }
  
  
  private void globalUpdateLeaseInfo(ZkLeaseInfo li) throws IOException {
    String node = buildLeaseInfoPath(li.holder);
    ZKClient.getInstance().setData(node, li.toByteArray());
  }
  
  private List<String> globalGetLeaseClient() throws IOException {
    List<String> ret = new ArrayList<String>();
    List<String> holders = ZKClient.getInstance().getChildren(
        FSConstants.ZOOKEEPER_LEASE_HOME, null);
    if (holders != null) {
      for (String holder : holders) {
        if (holder != null
            && !holder.startsWith(MONITOR_LOCK_PREFIX)) {
          ret.add(holder);
        }
      }
    }
    return ret;
  }
  
  private List<String> globalGetLeaseFiles(String holder) throws IOException {
    String node = buildLeaseInfoPath(holder);
    return ZKClient.getInstance().getChildren(node, null);
  }
  
  private void globalAddLeaseFile(String holder, int fileId) throws IOException {
    String node = buildLeaseFilePath(holder, fileId);
    ZKClient.getInstance().create(node, new byte[0], false, false);
  }
  
  private String globalFindHolder(int fileId) throws IOException {
    String path = buildFilePath(fileId);
    byte[] data = ZKClient.getInstance().getData(path, null);
    if(data == null) {
      // pending file doesn't exist
      return null; 
    } else {
      String strData = new String(data, CHARSET);
      String[] datas = strData.split(DbNodePendingFile.SEPARATOR);
      return datas[0]; // it is the client name
    }
  }
  
  private SortedSet<Lease> globalGetLeases() throws IOException {
    SortedSet<Lease> sortedLeases = new TreeSet<Lease>();
    List<String> clients = globalGetLeaseClient();
    if(clients != null) {
      for(String client : clients) {
        ZkLeaseInfo li = globalGetLeaseInfo(client, true);
        if(li != null) {
          sortedLeases.add(li);
        }
      }
    }
    return sortedLeases;
  }
  
  private String buildLeaseInfoPath(String holder) {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_LEASE_HOME);
    sb.append(SLASH).append(holder);
    return sb.toString();
  }
  
  private String buildLeaseFilePath(String holder, int fileId) {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_LEASE_HOME);
    sb.append(SLASH).append(holder).append(SLASH).append(fileId);
    return sb.toString();
  }
  
  private String buildFilePath(int fileId) {
    StringBuilder sb = new StringBuilder(FSConstants.ZOOKEEPER_FILE_HOME);
    sb.append(SLASH).append(fileId);
    return sb.toString();    
  }
  
  class ZkLeaseInfo extends Lease {
    
    private static final String SEPARATOR = ";";
    
    private ZkLeaseInfo(byte[] data, boolean loadLeaseFiles) throws IOException {
      super(null);
      set(data);
      if(loadLeaseFiles) {
        loadLeaseFiles();
      }
    }
    
    private ZkLeaseInfo(String clientName) {
      super(clientName);
    }
    
    private void set(byte[] data) {
      String strData = new String(data, CHARSET);
      String[] datas = strData.split(SEPARATOR);
      holder = datas[0];
      lastUpdate = Long.parseLong(datas[1]);
      strData = null;
    }
    
    private void loadLeaseFiles() throws IOException {
      fileIds.addAll(globalGetLeaseFiles(holder));
    }
    
    private byte[] toByteArray() {
      StringBuilder sb = new StringBuilder();
      sb.append(holder)
        .append(SEPARATOR).append(lastUpdate);
      return sb.toString().getBytes(CHARSET);
    }
  }
}
