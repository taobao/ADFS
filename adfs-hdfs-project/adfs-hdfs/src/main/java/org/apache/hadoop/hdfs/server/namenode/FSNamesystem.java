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

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.IPFromStorageIDUtil;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.metrics.TimeTracker;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMetrics;
import org.apache.hadoop.hdfs.server.namenode.metrics.TimeTrackerAPI;
import org.apache.hadoop.hdfs.server.namenode.metrics.TimeTrackerFactory;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.*;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.ZKUnderReplicatedBlocksImp.BlockIterator;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.io.IOUtils;

import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.file.FileProtocol;
import com.taobao.adfs.state.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.security.auth.login.LoginException;

/***************************************************
 * FSNamesystem does the actual bookkeeping work for the
 * DataNode.
 *
 * It tracks several important tables.
 *
 * 1)  valid fsname --> blocklist  (kept on disk, logged)
 * 2)  Set of all valid blocks (inverted #1)
 * 3)  block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4)  machine --> blocklist (inverted #2)
 * 5)  LRU cache of updated-heartbeat machines
 ***************************************************/
public class FSNamesystem implements FSConstants, FSNamesystemMBean {
  
  /** The handle to communicate with blocksmap service */
  InetSocketAddress bmAddress = null;
  
  /** The storage info and the namespace info */
  private NamespaceInfo nsInfo = null;
  
  // heartbeatRecheckInterval is how often namenode checks for expired datanodes
  private long datanodeMapUpdateInterval;
  
  // heartbeatExpireInterval is how long namenode waits for datanode to report
  private long datanodeExpireInterval;
  
  Daemon dnUpThread = null;   // DatanodeUpdate Thread  
  
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);
  public static final String AUDIT_FORMAT =
    "ugi=%s\t" +  // ugi
    "ip=%s\t" +   // remote IP
    "cmd=%s\t" +  // command
    "src=%s\t" +  // src path
    "dst=%s\t" +  // dst path (optional)
    "perm=%s";    // permissions (optional)

  private static final ThreadLocal<Formatter> auditFormatter =
    new ThreadLocal<Formatter>() {
      protected Formatter initialValue() {
        return new Formatter(new StringBuilder(AUDIT_FORMAT.length() * 4));
      }
  };

  private static final void logAuditEvent(UserGroupInformation ugi,
      InetAddress addr, String cmd, String src, String dst,
      FileStatus stat) {
    final Formatter fmt = auditFormatter.get();
    ((StringBuilder)fmt.out()).setLength(0);
    auditLog.info(fmt.format(AUDIT_FORMAT, ugi, addr, cmd, src, dst,
                  (stat == null)
                    ? null
                    : stat.getOwner() + ':' + stat.getGroup() + ':' +
                      stat.getPermission()
          ).toString());

  }

  public static final Log auditLog = LogFactory.getLog(
      FSNamesystem.class.getName() + ".audit");
  
  // Default initial capacity and load factor of map
  public static final int DEFAULT_INITIAL_MAP_CAPACITY = 16;
  public static final float DEFAULT_MAP_LOAD_FACTOR = 0.75f;

  private boolean isPermissionEnabled;
  private UserGroupInformation fsOwner;
  private String supergroup;
  private PermissionStatus defaultPermission;
  
  // FSNamesystemMetrics counter variables
  private FSNamesystemMetrics myFSMetrics;
  private long capacityTotal = 0L, capacityUsed = 0L, capacityRemaining = 0L;
  private int totalLoad = 0;

  volatile long pendingReplicationBlocksCount = 0L;
  volatile long corruptReplicaBlocksCount = 0L;
  volatile long underReplicatedBlocksCount = 0L;
  volatile long scheduledReplicationBlocksCount = 0L;
  volatile long excessBlocksCount = 0L;
  volatile long pendingDeletionBlocksCount = 0L;

  // Store blocks-->datanodedescriptor(s) map of corrupt replicas
  public AbsCorruptReplicasMap corruptReplicas;
    
  /**
   * Stores the datanode -> block map.  
   * <p>
   * Done by storing a set of {@link DatanodeDescriptor} objects, sorted by 
   * storage id. In order to keep the storage map consistent it tracks 
   * all storages ever registered with the namenode.
   * A descriptor corresponding to a specific storage id can be
   * <ul> 
   * <li>added to the map if it is a new storage id;</li>
   * <li>updated with a new datanode started as a replacement for the old one 
   * with the same storage id; and </li>
   * <li>removed if and only if an existing datanode is restarted to serve a
   * different storage id.</li>
   * </ul> <br>
   * The list of the {@link DatanodeDescriptor}s in the map is checkpointed
   * in the namespace image file. Only the {@link DatanodeInfo} part is 
   * persistent, the list of blocks is restored from the datanode block
   * reports. 
   * <p>
   * Mapping: StorageID -> DatanodeDescriptor
   */
  NavigableMap<String, DatanodeDescriptor> datanodeMap = 
    new TreeMap<String, DatanodeDescriptor>();

  // Keeps a Collection for every named machine containing
  // blocks that have recently been invalidated and are thought to live
  // on the machine in question.
  // Mapping: StorageID -> ArrayList<Block>
  private AbsRecentInvalidateSets recentInvalidateSets;

  // Keeps a TreeSet for every named node.  Each treeset contains
  // a list of the blocks that are "extra" at that location.  We'll
  // eventually remove these extras.
  // Mapping: StorageID -> TreeSet<Block>
  AbsExcessReplicationMap excessReplicateMap;

  Random r = new Random();

  /**
   * Stores a set of DatanodeDescriptor objects.
   * This is a subset of {@link #datanodeMap}, containing nodes that are 
   * considered alive.
   * The {@link HeartbeatMonitor} periodically checks for outdated entries,
   * and removes them from the list.
   */
  // A better concurrent data structure than ArrayList with intensive contain operation
  NavigableSet<DatanodeDescriptor> heartbeats = new ConcurrentSkipListSet<DatanodeDescriptor>();
  // Store set of Blocks that need to be replicated 1 or more times.
  // We also store pending replication-orders.
  // Set of: Block
  private AbsUnderReplicatedBlocks neededReplications;
  private AbsPendingReplicationBlocks pendingReplications;

  public DbNodeManager fileManager;
  public LeaseManager leaseManager;
  
  // A namenode-wide thread pool to asynchronously handle operations
  final int PROCESSORS = Runtime.getRuntime().availableProcessors();
  private final ExecutorService asynchronousProcessor = 
    new ThreadPoolExecutor(PROCESSORS, PROCESSORS*5,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new ThreadPoolExecutor.CallerRunsPolicy());

  // Threaded object that checks to see if we have been
  // getting heartbeats from all clients. 
  Daemon hbthread = null;   // HeartbeatMonitor thread
  public Daemon lmthread = null;   // LeaseMonitor thread
  Daemon smmthread = null;  // SafeModeMonitor thread
  public Daemon replthread = null;  // Replication thread
  
  private volatile boolean fsRunning = true;
  long systemStart = 0;

  //  The maximum number of replicates we should allow for a single block
  private volatile int maxReplication;
  //  How many outgoing replication streams a given node should have at one time
  private volatile int maxReplicationStreams;
  // MIN_REPLICATION is how many copies we need in place or else we disallow the write
  private volatile int minReplication;
  // Default replication
  private volatile int defaultReplication;
  // Variable to stall new replication checks for testing purposes
  private volatile boolean stallReplicationWork = false;
  // heartbeatRecheckInterval is how often namenode checks for expired datanodes
  private volatile long heartbeatRecheckInterval;
  //replicationRecheckInterval is how often namenode checks for new replication work
  private volatile long replicationRecheckInterval;
  // default block size of a file
  private volatile long defaultBlockSize = 0;
  // allow appending to hdfs files
  private volatile boolean supportAppends = true;
  // blockReportInterval is how often datanode sends block report
  private volatile long blockReportInterval;
  // heartbeatInterval is how often 
  private volatile long heartbeatInterval;
  // enable time tracker for perftest
  private boolean enableTimeTracker;
  /**
   * Last block index used for replication work.
   */
  private int replIndex = 0;
  private long missingBlocksInCurIter = 0;
  private long missingBlocksInPrevIter = 0; 

  public static FSNamesystem fsNamesystemObject;
  
  /** Safe Mode Information: 
   *  if minReplication > datanodemap.size, true; else false
   * */
  final AtomicBoolean safeMode = new AtomicBoolean(true);  
  
  /** NameNode RPC address */
  private InetSocketAddress nameNodeAddress = null;

  private Host2NodesMap host2DataNodeMap = new Host2NodesMap();
    
  // datanode networktoplogy
  NetworkTopology clusterMap = new NetworkTopology();
  private DNSToSwitchMapping dnsToSwitchMapping;
  
  // for block replicas placement
  ReplicationTargetChooser replicator;

  private HostsFileReader hostsReader; 
  private Daemon dnthread = null;

  private long maxFsObjects = 0;          // maximum number of fs objects

  /**
   * The global generation stamp for this file system. 
   */
  private ZKGenerationStamp generationStamp;

  // Ask Datanode only up to this many blocks to delete.
  private int blockInvalidateLimit = FSConstants.BLOCK_INVALIDATE_CHUNK;

  // precision of access times.
  private long accessTimePrecision = 0;

  static Random randBlockId = new Random(); 
  
  StateManager stateManager;
  DbLockManager lockManager;
  
  private ExternalDatanodeCommandsHandler externalDatanodeCommandsHandler;
  
  private TrashCleaner trasher;
  
  /**
   * FSNamesystem constructor.
   */
  FSNamesystem(NameNode nn, Configuration conf) throws IOException {
    try {
      initialize(nn, conf);
    } catch(Exception e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      close();
      throw new IOException(e);
    }
  }
  
  /**
   * Initializes some of the members from configuration
   */
  private void setConfigurationParameters(Configuration conf) 
                                          throws IOException {
    fsNamesystemObject = this;
    try {
      fsOwner = UnixUserGroupInformation.login(conf);
    } catch (LoginException e) {
      throw new IOException(StringUtils.stringifyException(e));
    }
    LOG.info("fsOwner=" + fsOwner);

    this.supergroup = conf.get("dfs.permissions.supergroup", "supergroup");
    this.isPermissionEnabled = conf.getBoolean("dfs.permissions", true);
    LOG.info("supergroup=" + supergroup);
    LOG.info("isPermissionEnabled=" + isPermissionEnabled);
    short filePermission = (short)conf.getInt("dfs.upgrade.permission", 0777);
    this.defaultPermission = PermissionStatus.createImmutable(
        fsOwner.getUserName(), supergroup, new FsPermission(filePermission));


    this.replicator = new ReplicationTargetChooser(
                         conf.getBoolean("dfs.replication.considerLoad", true),
                         this,
                         clusterMap);
    this.defaultReplication = conf.getInt("dfs.replication", 3);
    this.maxReplication = conf.getInt("dfs.replication.max", 512);
    this.minReplication = conf.getInt("dfs.replication.min", 1);
    if (minReplication <= 0)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.min = " 
                            + minReplication
                            + " must be greater than 0");
    if (maxReplication >= (int)Short.MAX_VALUE)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.max = " 
                            + maxReplication + " must be less than " + (Short.MAX_VALUE));
    if (maxReplication < minReplication)
      throw new IOException(
                            "Unexpected configuration parameters: dfs.replication.min = " 
                            + minReplication
                            + " must be less than dfs.replication.max = " 
                            + maxReplication);
    this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);
    this.heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
    this.heartbeatRecheckInterval = conf.getInt("heartbeat.recheck.interval", 5 * 60 * 1000); // 5 minutes
    this.datanodeMapUpdateInterval = conf.getInt("datanodemap.update.interval", 3 * 60 * 1000); // 3 minutes
    if (heartbeatRecheckInterval <= datanodeMapUpdateInterval) {
    	
      throw new IOException(
          "Unexpected configuration parameters: datanodemap.update.interval = "
              + datanodeMapUpdateInterval
              + " must be less than heartbeat.recheck.interval = "
              + heartbeatRecheckInterval);
    }
    this.datanodeExpireInterval = 2 * heartbeatRecheckInterval + 10 * heartbeatInterval;
    this.replicationRecheckInterval = conf.getInt("dfs.replication.interval", 3) * 1000L;
    this.defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    this.maxFsObjects = conf.getLong("dfs.max.objects", 0);
    this.blockInvalidateLimit = Math.max(this.blockInvalidateLimit, 
                                         20*(int)(heartbeatInterval/1000));
    this.accessTimePrecision = conf.getLong("dfs.access.time.precision", 0);
    this.supportAppends = conf.getBoolean("dfs.support.append", false);
    this.blockReportInterval =
      conf.getLong("dfs.blockreport.intervalMsec", BLOCKREPORT_INTERVAL);
    this.enableTimeTracker=conf.getBoolean("test.time.tracker", false);
 }
  
  /**
   * Initialize FSNamesystem.
   */
  private void initialize(NameNode nn, Configuration conf) throws Exception {
    this.systemStart = now();
    conf.set("distributed.data.path", "namespace");
    setConfigurationParameters(conf);
    this.nameNodeAddress = nn.getNameNodeAddress();
    this.registerMBean(conf);
    stateManager = new StateManager(conf);
    lockManager = new DbLockManager(stateManager, 
        conf.getLong("state.lock.expire", 60000), 
        conf.getLong("state.lock.timeout", 30000));
      
    generationStamp = new ZKGenerationStamp(lockManager);
    corruptReplicas = new ZKCorruptReplicasMapImp();
    excessReplicateMap = new ZKExcessReplicationMapImp();
    pendingReplications = new ZKPendingReplicationBlocksImp(lockManager,
        conf.getInt("dfs.replication.pending.timeout.sec", -1) * 1000L);
    neededReplications = new ZKUnderReplicatedBlocksImp();
    recentInvalidateSets = new ZKRecentInvalidateSetsImp(this, heartbeatInterval, blockReportInterval);

    fileManager = new DbNodeManager(this);
    leaseManager = new LeaseManager(this);
    
    nsInfo = new NamespaceInfo(1, now()); // TODO impl it make more sense
    
    this.hostsReader = new HostsFileReader(conf.get("dfs.hosts",""), conf.get("dfs.hosts.exclude",""));
    this.dnthread = new Daemon(new DecommissionManager(this).new Monitor(
        conf.getInt("dfs.namenode.decommission.interval", 30),
        conf.getInt("dfs.namenode.decommission.nodes.per.interval", 5)));
    dnthread.start();
    
    this.dnsToSwitchMapping = ReflectionUtils.newInstance(
        conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
            DNSToSwitchMapping.class), conf);
    
    /* If the dns to swith mapping supports cache, resolve network 
     * locations of those hosts in the include list, 
     * and store the mapping in the cache; so future calls to resolve
     * will be fast.
     */
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      dnsToSwitchMapping.resolve(new ArrayList<String>(hostsReader.getHosts()));
    }
    
    externalDatanodeCommandsHandler = 
      new ExternalDatanodeCommandsHandler(this, conf);
    
    updateDatanodesMap(false);
    
    this.dnUpThread = new Daemon(new DatanodeUpdateThread());
    dnUpThread.start();
    
    this.hbthread = new Daemon(new HeartbeatMonitor());
    hbthread.start();
    
    this.lmthread = new Daemon(leaseManager.new Monitor());
    lmthread.start();
    
    this.replthread = new Daemon(new ReplicationMonitor());
    replthread.start();
    
    this.trasher = new TrashCleaner(this);
  }
  
  /**
   * Close down this file system manager.
   * Causes heartbeat and lease daemons to stop; waits briefly for
   * them to finish, but a short timeout returns control back to caller.
   */
  public void close() {
    fsRunning = false;
    try {
      if (pendingReplications != null) pendingReplications.stop();
      if (recentInvalidateSets != null) recentInvalidateSets.stop();
      if (hbthread != null) hbthread.interrupt();
      if (replthread != null) replthread.interrupt();
      if (dnthread != null) dnthread.interrupt();
      if (externalDatanodeCommandsHandler != null) externalDatanodeCommandsHandler.stop();
      if (trasher != null) trasher.close();
    } catch (Exception e) {
      LOG.warn("Exception shutting down FSNamesystem", e);
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        if (asynchronousProcessor != null) {
          asynchronousProcessor.shutdown();
          asynchronousProcessor.awaitTermination(30, TimeUnit.SECONDS);
        }
        if (lmthread != null) {
          lmthread.interrupt();
          lmthread.join(3000);
        }
        if(stateManager != null) {
          stateManager.close();
        }
      } catch (InterruptedException ie) {
      } catch (IOException ie) {
        LOG.error("Error closing FSDirectory", ie);
        IOUtils.cleanup(LOG, stateManager);
      }
    }
  }
  
  /**
   * get FSNamesystemMetrics
   */
  public FSNamesystemMetrics getFSNamesystemMetrics() {
    return myFSMetrics;
  }
  
  
  /**
   * shutdown FSNamesystem
   */
  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }
  

  /**
   * Gets the generation stamp for this filesystem
   */
  public long getGenerationStamp() throws IOException {
    return generationStamp.getStamp();
  }
  
  /**
   * Check if there are any expired datanode, and if so,
   * mark it as dead and 
   */
  void heartbeatCheck() {
    boolean allAlive = false;
    while (!allAlive) {
      boolean foundDead = false;
      DatanodeID nodeID = null;

      /* Here we do the 'GLOBAL' heartbeat health check,
      not limit to those report to this name node  */     
      synchronized (datanodeMap) {
        for (DatanodeDescriptor dnDesc : datanodeMap.values()) {
          if (isDatanodeDead(dnDesc)) {
            nodeID = dnDesc;
            foundDead = true;
            break;
          }
        }
      }
      if (foundDead) {
        // acquire the fsnamesystem lock, and then remove the dead node.
        DatanodeDescriptor nodeInfo = null;
        try {
          nodeInfo = getDatanode(nodeID);
        } catch (IOException e) {
          nodeInfo = null;
        }
        if (nodeInfo != null && isDatanodeDead(nodeInfo)) {
          NameNode.stateChangeLog.info("BLOCK* NameSystem.heartbeatCheck: "
              + "lost heartbeat from " + nodeInfo.getName());
          handleRemoveDatanode(nodeInfo);
        }
      }
      allAlive = !foundDead;
    }
  }
  
  /**
   * This method is used by ExternalDatanodeCommandsHandler
   * in case the failure of RPC.
   * @param isa
   */
  void removeExternalDatanode(InetSocketAddress isa) {
    DatanodeID toRemove = null;
    synchronized (datanodeMap) {
      for(DatanodeDescriptor dnDesc : datanodeMap.values()) {
        if(dnDesc.getHost() == isa.getHostName() &&
            dnDesc.getIpcPort() == isa.getPort()) {
          toRemove = dnDesc;
          break;
        }
      }
    }
    if(toRemove != null) {
      DatanodeDescriptor nodeInfo = null;
      try {
        nodeInfo = getDatanode(toRemove);
      } catch (IOException e) {
        nodeInfo = null;
      }
      if(nodeInfo != null) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.removeExternalDatanode: "
            + "cannot setup RPC to " + nodeInfo.getName());
        handleRemoveDatanode(nodeInfo);
      }
    }
  }
  
  void handleRemoveDatanode(DatanodeDescriptor dnDesc) {
    removeDatanode(dnDesc);
  }
  
  void updateHeartbeatInBatchForBM() {
    Map<DatanodeRegistration, DatanodeInfo> regMap = 
      new LinkedHashMap<DatanodeRegistration, DatanodeInfo>();
    synchronized (heartbeats) {
      LOG.info("updateHeartbeatInBatchForBM starts. " +
          " heartbeats size=" + heartbeats.size());
      /* for performance sake, we copy from heartbeats 
         and leave synchronized block earlier*/
      for (DatanodeDescriptor dnDesc : heartbeats) {
        DatanodeRegistration dnReg = new DatanodeRegistration(dnDesc.getName());
        dnReg.setStorageID(dnDesc.getStorageID());
        dnReg.setInfoPort(dnDesc.getInfoPort());
        dnReg.setIpcPort(dnDesc.getIpcPort());
        regMap.put(dnReg, new DatanodeInfo(dnDesc));
      } // for end
    } // synchronized end
    
    //invoke handleHeartbeat out of synchronization  
    Iterator<DatanodeRegistration> iter = regMap.keySet().iterator();
    if(iter != null) {
      DatanodeRegistration dnReg = null;
      DatanodeInfo dnInfo = null;
      for(;iter.hasNext();) {
        dnReg = iter.next();
        dnInfo = regMap.get(dnReg);
        try {
          boolean res = stateManager.handleHeartbeat(dnReg, dnInfo.getCapacity(),
              dnInfo.getDfsUsed(), dnInfo.getRemaining(), dnInfo.getXceiverCount(),
              dnInfo.getLastUpdate(),
              DatanodeInfo.AdminStates.NORMAL);
          if(res){
            LOG.info("updateHeartbeatInBatchForBM from " + dnReg + " successed");
          } else {
            LOG.info("updateHeartbeatInBatchForBM from " + dnReg + " failed");
            /* we intend to set this flag to false to let it re-register in
             * handleHeartbeat for abnormal situations. */
            synchronized(datanodeMap) {
              DatanodeDescriptor dnDesc = datanodeMap.get(dnReg.getStorageID());
              dnDesc.isAlive = false;
            } 
          }
        } catch (IOException ioe) {
          LOG.error("updateHeartbeatInBatchForBM from " + dnReg + " failed", ioe);
          /* we intend to set this flag to false to let it re-register in
           * handleHeartbeat for abnormal situations. */
          synchronized(datanodeMap) {
            DatanodeDescriptor dnDesc = datanodeMap.get(dnReg.getStorageID());
            dnDesc.isAlive = false;
          }
        }
      } // for loop ends
    } // iteration ends
  }
  
  /**
   * Periodically update datanode status from bm service 
   */
  class DatanodeUpdateThread implements Runnable {
    /**
     */
    public void run() {
      while (fsRunning) {
        try {
          updateHeartbeatInBatchForBM(); // push my data
          updateDatanodesMap(true); // pull all data
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        }
        try {
          Thread.sleep(datanodeMapUpdateInterval);
        } catch (InterruptedException ie) {
        }
      }
    }
  }
  
  /**
   * Periodically check out-of-date datanode in datanodeMap 
   */
  class HeartbeatMonitor implements Runnable {
    /**
     */
    public void run() {
      boolean firstRun = true;
      while (fsRunning) {
        try {
          /* skip the first loop of heartbeat check to wait for 
             the late join datanodes registered before */
          if(firstRun) {
            firstRun = false;
            LOG.info("skip the first loop of heartbeat check to wait for the " +
                "late join datanodes registered before. zzzz..");
          }else {
            heartbeatCheck();
          }
        } catch (Exception e) {
          LOG.error(StringUtils.stringifyException(e));
        }
        try {
          Thread.sleep(heartbeatRecheckInterval);
        } catch (InterruptedException ie) {
        }
      }
    }
  }
  
  
  
  /**
   * Periodically calls computeReplicationWork().
   */
  class ReplicationMonitor implements Runnable {
    static final int INVALIDATE_WORK_PCT_PER_ITERATION = 32;
    static final float REPLICATION_WORK_MULTIPLIER_PER_ITERATION = 2;
    
    public void run() {
      while (fsRunning) {
        try {
          /* use a distributed lock to secure only one namenode can 
           * perform replication monitor at one time. the drawback is 
           * if this namenode is crashed, others will wait for 
           * the corresponding node on zookeeper disappeared. However,
           * I think the advantages outweighs drawbacks.
          */
          if(lockManager.trylockRepMonitor()) { 
            try {
              computeDatanodeWork();
              processPendingReplications();
            } finally {
              lockManager.unlockRepMonitor();
            }
          } else {
            LOG.debug("Other namenode is performing replication monitor");
          }
          Thread.sleep(replicationRecheckInterval);
        } catch (InterruptedException ie) {
          LOG.warn("ReplicationMonitor thread received InterruptedException." + ie);
          break;
        } catch (IOException ie) {
          LOG.warn("ReplicationMonitor thread received IO exception. " + ie + " "
              + StringUtils.stringifyException(ie));
          if(ie.getMessage().contains("KeeperErrorCode = Session expired")) {
            // we cannot handle this kind of IOException from ZOOKEEPER
            Runtime.getRuntime().exit(-1);
          }
        } catch (Throwable t) {
          LOG.warn("ReplicationMonitor thread received Runtime exception. " + t
              + " " + StringUtils.stringifyException(t));
          t.printStackTrace();
          Runtime.getRuntime().exit(-1);
        }
      }
    }
  }

  /**
   * Compute block replication and block invalidation work that can be scheduled
   * on data-nodes. The datanode will be informed of this work at the next
   * heartbeat.
   * 
   * @return number of blocks scheduled for replication or removal.
   */
  public int computeDatanodeWork() throws IOException {
    int workFound = 0;
    int blocksToProcess = 0;
    int nodesToProcess = 0;
    // blocks should not be replicated or removed if safe mode is on
    if (safeMode.get())
      return workFound;
    synchronized(datanodeMap) {
      blocksToProcess = (int) (datanodeMap.size() * ReplicationMonitor.REPLICATION_WORK_MULTIPLIER_PER_ITERATION);
      nodesToProcess = (int) Math.ceil((double) datanodeMap.size()
          * ReplicationMonitor.INVALIDATE_WORK_PCT_PER_ITERATION / 100);
    }
    workFound = computeReplicationWork(blocksToProcess);


    pendingReplicationBlocksCount = pendingReplications.size();
    underReplicatedBlocksCount = neededReplications.size();
    scheduledReplicationBlocksCount = workFound;
    corruptReplicaBlocksCount = corruptReplicas.size();


    workFound += computeInvalidateWork(nodesToProcess);
    return workFound;
  }

  private int computeInvalidateWork(int nodesToProcess) {
    int blockCnt = 0;
    for (int nodeCnt = 0; nodeCnt < nodesToProcess; nodeCnt++) {
      int work = invalidateWorkForOneNode();
      if (work == 0)
        break;
      blockCnt += work;
    }
    return blockCnt;
  }

  /**
   * Scan blocks in {@link #neededReplications} and assign replication work to
   * data-nodes they belong to.
   * 
   * The number of process blocks equals either twice the number of live
   * data-nodes or the number of under-replicated blocks whichever is less.
   * 
   * @return number of blocks scheduled for replication during this iteration.
   */
  private int computeReplicationWork(int blocksToProcess) throws IOException {
    // stall only useful for unit tests (see TestFileAppend4.java)
    if (stallReplicationWork) {
      return 0;
    }

    // Choose the blocks to be replicated
    List<List<Block>> blocksToReplicate = chooseUnderReplicatedBlocks(blocksToProcess);

    // replicate blocks
    int scheduledReplicationCount = 0;
    for (int i = 0; i < blocksToReplicate.size(); i++) {
      for (Block block : blocksToReplicate.get(i)) {
        if (computeReplicationWorkForBlock(block, i)) {
          scheduledReplicationCount++;
        }
      }
    }
    return scheduledReplicationCount;
  }

  /**
   * Get a list of block lists to be replicated The index of block lists
   * represents the
   * 
   * @param blocksToProcess
   * @return Return a list of block lists to be replicated. The block list index
   *         represents its replication priority.
   */
  List<List<Block>> chooseUnderReplicatedBlocks(int blocksToProcess) {
    // initialize data structure for the return value
    List<List<Block>> blocksToReplicate = new ArrayList<List<Block>>(
        AbsUnderReplicatedBlocks.LEVEL);
    for (int i = 0; i < AbsUnderReplicatedBlocks.LEVEL; i++) {
      blocksToReplicate.add(new ArrayList<Block>());
    }

    int neededRepSize = neededReplications.size();
    if (neededRepSize == 0) {
      missingBlocksInCurIter = 0;
      missingBlocksInPrevIter = 0;
      return blocksToReplicate;
    }

    // Go through all blocks that need replications.
    BlockIterator neededReplicationsIterator = neededReplications.iterator();
    // skip to the first unprocessed block, which is at replIndex
    for (int i = 0; i < replIndex && neededReplicationsIterator.hasNext(); i++) {
      neededReplicationsIterator.next();
    }
    // # of blocks to process equals either twice the number of live
    // data-nodes or the number of under-replicated blocks whichever is less
    blocksToProcess = Math.min(blocksToProcess, neededRepSize);

    for (int blkCnt = 0; blkCnt < blocksToProcess; blkCnt++, replIndex++) {
      if (!neededReplicationsIterator.hasNext()) {
        // start from the beginning
        replIndex = 0;
        missingBlocksInPrevIter = missingBlocksInCurIter;
        missingBlocksInCurIter = 0;
        blocksToProcess = Math
            .min(blocksToProcess, neededRepSize);
        if (blkCnt >= blocksToProcess)
          break;
        neededReplicationsIterator = neededReplications.iterator();
        // assert neededReplicationsIterator.hasNext() : "neededReplications should not be empty.";
        if(!neededReplicationsIterator.hasNext()) {
          LOG.warn("neededReplications should not be empty.");
          continue;
        }
      }
      
      Block block = neededReplicationsIterator.next();
      int priority = neededReplicationsIterator.getPriority();
      if (priority < 0 || priority >= blocksToReplicate.size()) {
        LOG.warn("Unexpected replication priority: " + priority + " " + block);
      } else {
        blocksToReplicate.get(priority).add(block);
      }
    } // end for
    return blocksToReplicate;
  }

  /**
   * Replicate a block
   * 
   * @param block block to be replicated
   * @param priority a hint of its priority in the neededReplication queue
   * @return if the block gets replicated or not
   * @throws IOException
   */
  boolean computeReplicationWorkForBlock(Block block, int priority)
      throws IOException {
    int requiredReplication, numEffectiveReplicas;
    List<DatanodeDescriptor> containingNodes = null;
    DatanodeDescriptor srcNode;

    if(!lockManager.lockBlock(block.getBlockId())) {
      throw new IOException("cannot get block lock for block=" + block);
    }
    try {
      BlockEntry blockEntry = stateManager.getStoredBlockBy(block.getBlockId());
      if (blockEntry == null) {
        neededReplications.remove(block, priority);
        return false;
      }
      DbNodeFile file = fileManager.getFile(blockEntry.getFileId());
      if (file == null || file.isUnderConstruction()) {
        neededReplications.remove(block, priority); // remove from
                                                    // neededReplications
        replIndex--;
        return false;
      }
      requiredReplication = file.getReplication();
  
      // get a source data-node
      containingNodes = new ArrayList<DatanodeDescriptor>();
      NumberReplicas numReplicas = new NumberReplicas();
      srcNode = chooseSourceDatanode(block, containingNodes, numReplicas);
      if ((numReplicas.liveReplicas() + numReplicas.decommissionedReplicas()) <= 0) {
        missingBlocksInCurIter++;
      }
      if (srcNode == null) // block can not be replicated from any node
        return false;
  
      // do not schedule more if enough replicas is already pending
      numEffectiveReplicas = numReplicas.liveReplicas()
          + pendingReplications.getNumReplicas(block);
      if (numEffectiveReplicas >= requiredReplication) {
        neededReplications.remove(block, priority); // remove from
                                                    // neededReplications
        replIndex--;
        NameNode.stateChangeLog.info("BLOCK* " + "Removing block " + block
            + " from neededReplications as it has enough replicas.");
        return false;
      }
      
      // choose replication targets:
      // It's SAFE in fine-grained lock of adfs, not in "GLOBAL LOCK"
      DatanodeDescriptor targets[] = replicator.chooseTarget(requiredReplication
          - numEffectiveReplicas, srcNode, containingNodes, null,
          block.getNumBytes());
      if (targets.length == 0)
        return false;
      
      // Before adding block to the to be replicated list, update block data
      if(block.getGenerationStamp() < blockEntry.getGenerationStamp()) {
        LOG.warn("block gentStamp from neededReplications might be out of date, " +
            block.getGenerationStamp() + 
            ", actually is " + blockEntry.getGenerationStamp());
        block.setGenerationStamp(blockEntry.getGenerationStamp());
      }
      srcNode.addBlockToBeReplicated(block, targets);

      for (DatanodeDescriptor dn : targets) {
        dn.incBlocksScheduled();
      }

      // Move the block-replication into a "pending" state.
      // The reason we use 'pending' is so we can retry
      // replications that fail after an appropriate amount of time.
      pendingReplications.add(block, targets.length);
      NameNode.stateChangeLog.debug("BLOCK* block " + block
          + " is moved from neededReplications to pendingReplications");

      // remove from neededReplications
      if (numEffectiveReplicas + targets.length >= requiredReplication) {
        neededReplications.remove(block, priority); // remove from neededReplications
        replIndex--;
      }
      if (NameNode.stateChangeLog.isInfoEnabled()) {
        StringBuffer targetList = new StringBuffer("datanode(s)");
        for (int k = 0; k < targets.length; k++) {
          targetList.append(' ');
          targetList.append(targets[k].getName());
        }
        NameNode.stateChangeLog.info("BLOCK* ask " + srcNode.getName()
            + " to replicate " + block + " to " + targetList);
        NameNode.stateChangeLog.debug("BLOCK* neededReplications = "
            + neededReplications.size() + " pendingReplications = "
            + pendingReplications.size());
      }
    } finally {
      lockManager.unlockBlock(block.getBlockId());
    }

    if(!heartbeats.contains(srcNode)) {
      LOG.info("This is an external data node, " +
          "use externalDatanodeCommandsHandler to handle transfer");
      externalDatanodeCommandsHandler.transferBlock(srcNode, 1);
    }else{
      LOG.info("This is a heartbeated data node, " +
      "use the normal way!");
    }
    return true;
  }

  /**
   * Parse the data-nodes the block belongs to and choose one, which will be the
   * replication source.
   * 
   * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
   * since the former do not have write traffic and hence are less busy. We do
   * not use already decommissioned nodes as a source. Otherwise we choose a
   * random node among those that did not reach their replication limit.
   * 
   * In addition form a list of all nodes containing the block and calculate its
   * replication numbers.
   */
  private DatanodeDescriptor chooseSourceDatanode(Block block,
      List<DatanodeDescriptor> containingNodes, NumberReplicas numReplicas) {
    containingNodes.clear();
    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int corrupt = 0;
    int excess = 0;

    Collection<Integer> itDnIndex = null;
    try {
      itDnIndex = stateManager.getDatanodeList(block.getBlockId());
    } catch (IOException e) {
      LOG.error("chooseSourceDatanode: getDatanodeList failed for " 
          + block.getBlockId(), e);
    }
    if(itDnIndex == null) {
      return null;
    }
    Iterator<Integer> it = itDnIndex.iterator();
    boolean exist = false;
    boolean corruptExist=false;
    while (it.hasNext()) {
      DatanodeDescriptor node = getDataNodeDescriptorByID(it.next());
      if (node == null) continue;
      try {
        exist = excessReplicateMap.exist(node.getStorageID(), block);
        corruptExist=corruptReplicas.isReplicaCorrupt(block, node);
      } catch (IOException e) {
        LOG.error("chooseSourceDatanode: IOException occurs for block=" + block +
            ", and nodeId=" + node, e);
        continue;
      }
      if(corruptExist)
        corrupt++;
      else if (node.isDecommissionInProgress() || node.isDecommissioned())
        decommissioned++;
      else if (exist) {
        excess++;
      } else {
        live++;
      }
      containingNodes.add(node);
      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if(corruptExist)
        continue;
      if (node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams)
        continue; // already reached replication limit
      // the block must not be scheduled for removal on srcNode
      if (exist)
        continue;
      // never use already decommissioned nodes
      if (node.isDecommissioned())
        continue;
      // we prefer nodes that are in DECOMMISSION_INPROGRESS state
      // we also prefer the node reports to this name node :-)
      if (node.isDecommissionInProgress() || heartbeats.contains(node) ||
          srcNode == null) {
        srcNode = node;
        continue;
      }
      if (srcNode.isDecommissionInProgress())
        continue;
      if (heartbeats.contains(srcNode))
        continue;
      // switch to a different node randomly
      // this to prevent from deterministically selecting the same node even
      // if the node failed to replicate the block on previous iterations
      if (r.nextBoolean())
        srcNode = node;
    }
    if (numReplicas != null)
      numReplicas.initialize(live, decommissioned, corrupt, excess);
    return srcNode;
  }

  /**
   * Get blocks to invalidate for the first node in
   * {@link #recentInvalidateSets}.
   * 
   * @return number of blocks scheduled for removal during this iteration.
   */
  private int invalidateWorkForOneNode() {
    // blocks should not be replicated or removed if safe mode is on
    if (safeMode.get())
      return 0;
    
    // get blocks to invalidate for the first node
    String firstNodeId = null;
    try{
      firstNodeId = recentInvalidateSets.getFirstNodeId();
    }catch(IOException e1) {
      e1.printStackTrace();
    }
    
    if(firstNodeId == null) {
      return 0;
    }

    DatanodeDescriptor dn = datanodeMap.get(firstNodeId);
    if (dn == null) {
      LOG.error("failed to get datanode descriptor for " + firstNodeId );
      removeFromInvalidates(firstNodeId);
      return 0;
    }

    LOG.info("Now, handle invalidate work for node " + firstNodeId);
    
    ArrayList<Block> blocksToInvalidate = null;
    
    try {
      blocksToInvalidate =
        recentInvalidateSets.getBlocks(firstNodeId,blockInvalidateLimit);
    } catch (IOException e){
      LOG.error("invalidateWorkForOneNode: " +
      		"recentInvalidateSets.getBlocks failed for blkId=" + firstNodeId, e);
    }
    
    if(blocksToInvalidate == null || 
        blocksToInvalidate.size() < 1) {
      return 0;
    }
    for (Block b : blocksToInvalidate) {
      try {
        recentInvalidateSets.remove(firstNodeId, b.getBlockName());
      } catch (IOException e) {
        LOG.error("Error happened when removing invalidate block " + 
                   b.getBlockName() + " from zookeeper", e);
      }
    }

    dn.addBlocksToBeInvalidated(blocksToInvalidate);

    if (NameNode.stateChangeLog.isInfoEnabled()) {
      StringBuffer blockList = new StringBuffer();
      for (Block blk : blocksToInvalidate) {
        blockList.append(' ');
        blockList.append(blk);
      }
      NameNode.stateChangeLog.info("BLOCK* ask " + dn.getName() + " to delete "
          + blockList);
    }
    
    int size = blocksToInvalidate.size();
    if(!heartbeats.contains(dn)) {
      LOG.info("This is an external data node, " +
      "use externalDatanodeCommandsHandler to handle invalidate");
      externalDatanodeCommandsHandler.invalidateBlocks(dn, size);
    }else{
      LOG.info("This is a heartbeated data node, " +
      "use the normal way!");
    }
    return size;
  }
  
  public void setNodeReplicationLimit(int limit) {
    this.maxReplicationStreams = limit;
  }

  /**
   * If there were any replication requests that timed out, reap them and put
   * them back into the neededReplication queue
   * 
   * @throws IOException
   */
  void processPendingReplications() throws IOException {
    Block[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      for (int i = 0; i < timedOutItems.length; i++) {
        try {
          NumberReplicas num = countNodes(timedOutItems[i]);
          if (num.isAllZero()) {
            // in this case, there is no entry in blockmap
            // so we need to remove it directly.
            pendingReplications.remove(timedOutItems[i]);
            LOG.info("numberreplicas's values are all zero, "
                    + "this kind of block should be removed from pendingReplications directly");
          }
          boolean ret = neededReplications.add(timedOutItems[i], num
              .liveReplicas(), num.decommissionedReplicas(),
              getReplication(timedOutItems[i]));
          if (!ret) {
            // curReplicas<0 || expectedReplicas<=curReplicas
            pendingReplications.remove(timedOutItems[i]);
          }
          /*
           * If we know the target datanodes where the replication timedout, we
           * could invoke decBlocksScheduled() on it. Its ok for now.
           */
        } catch (IOException ioe) {
          LOG.error("processPendingReplications: block=" + timedOutItems[i],
              ioe);
        } // end of try-catch
      } // end of loop
    } // end of if
  }

  /* get replication factor of a block */
  private int getReplication(Block block) throws IOException {
    BlockEntry blockEntry = stateManager.getStoredBlockBy(block.getBlockId());
    if (blockEntry == null) {
      return 0;
    }
    FileStatus fileStatus = null;
    try {
      fileStatus = fileManager.getFileStatus(blockEntry.getFileId());
    } catch (IOException e) {
      fileStatus = null;
    }

    if (fileStatus == null) { // block does not belong to any file
      return 0;
    }
    assert !fileStatus.isDir() : "Block cannot belong to a directory.";
    return fileStatus.getReplication();
  }
  
  /**
   * Start decommissioning the specified datanode. 
   */
  private void startDecommission (DatanodeDescriptor node) 
    throws IOException {

    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      LOG.info("Start Decommissioning node " + node.getName());
      node.startDecommission();
      //
      // all the blocks that reside on this node have to be 
      // replicated.
      int nodeid = IPFromStorageIDUtil.getIDFromStorageID(node.getStorageID());
      if( nodeid >= 0) {
	      Collection<BlockEntry> allBlocksOnDn = stateManager.getAllBlocksOnDatanode(nodeid);
	      if(allBlocksOnDn != null) {
	        Iterator<BlockEntry> decommissionBlocks = allBlocksOnDn.iterator();
	        while (decommissionBlocks.hasNext()) {
	          BlockEntry blockEntry = decommissionBlocks.next();
	          Block b = new Block(blockEntry.getBlockId(), blockEntry.getNumbytes(), blockEntry.getGenerationStamp());
	          updateNeededReplications(b, -1, 0, getReplication(b));
	        }
	      }
      }
    }
  }
  
  /**
   * Stop decommissioning the specified datanodes.
   */
  public void stopDecommission (DatanodeDescriptor node) 
    throws IOException {
    LOG.info("Stop Decommissioning node " + node.getName());
    node.stopDecommission();
  }
  
  private boolean isDatanodeDead(DatanodeInfo node) {

    boolean ret = (node.getLastUpdate() <
        (now() - datanodeExpireInterval));
    if(!ret) return ret; // this is a healthy node
    
    if(node.getLastUpdate() == -1) {
      // this should be a definite dead node
      return true;
    } else {
      String nodePath = FSConstants.ZOOKEEPER_DATANODE_GROUNP +"/"+ node.getStorageID();
      try {
        if(ZKClient.getInstance().exist(nodePath) != null) {
          /* the node has a path in zookeeper, should be healthy 
          or back to healthy, anyway let's set it to suspected value 
          if it's really a healthy node, the lastupdate value will be 
          refreshed during the next updatedatanodemap method.*/
          LOG.info("suspect node " + node.getStorageID() + " is dead");
          node.setLastUpdate(-1);
          return false;
        }
      } catch (IOException ioe) {
        LOG.error("Exception occurs when checking if node " 
            + node.getStorageID() + " is healthy on zookeeper");
      }
      return true;
    }
  }
  
  public static Collection<File> getNamespaceDirs(Configuration conf) {
    Collection<String> dirNames = conf.getTrimmedStringCollection("dfs.name.dir");
    if (dirNames.isEmpty())
      dirNames.add("/tmp/hadoop/dfs/name");
    Collection<File> dirs = new ArrayList<File>(dirNames.size());
    for(String name : dirNames) {
      dirs.add(new File(name));
    }
    return dirs;
  }
  
  public static Collection<File> getNamespaceEditsDirs(Configuration conf) {
    Collection<String> editsDirNames = 
            conf.getTrimmedStringCollection("dfs.name.edits.dir");
    if (editsDirNames.isEmpty())
      editsDirNames.add("/tmp/hadoop/dfs/name");
    Collection<File> dirs = new ArrayList<File>(editsDirNames.size());
    for(String name : editsDirNames) {
      dirs.add(new File(name));
    }
    return dirs;
  }
  
  UpgradeCommand processDistributedUpgradeCommand(UpgradeCommand comm) throws IOException {
    // TODO
    return null;
  }
  
  public static FSNamesystem getFSNamesystem() {
    return fsNamesystemObject;
  }
  
  NamespaceInfo getNamespaceInfo() {
    return nsInfo;
  }
  
  /**
   * Log a rejection of an addStoredBlock RPC, invalidate the reported block,
   * and return it.
   */
  private Block rejectAddStoredBlock(Block block, DatanodeDescriptor node,
      String msg) {
    NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
        + "addStoredBlock request received for " + block + " on "
        + node.getName() + " size " + block.getNumBytes()
        + " but was rejected: " + msg);
    addToInvalidates(block, node);
    return block;
  }
  
  /**
   * Invalidate corrupt replicas.
   * <p>
   * This will remove the replicas from the block's location list,
   * add them to {@link #recentInvalidateSets} so that they could be further
   * deleted from the respective data-nodes,
   * and remove the block from corruptReplicasMap.
   * <p>
   * This method should be called when the block has sufficient
   * number of live replicas.
   *
   * @param blk Block whose corrupt replicas need to be invalidated
   * @throws IOException 
   */
  void invalidateCorruptReplicas(Block blk) throws IOException {
    boolean gotException = false;
    List<String> nodes = null;
    nodes = corruptReplicas.getNodes(blk);
    if(nodes==null) return;
        
    // Make a copy of this list, since calling invalidateBlock will modify
    // the original (avoid CME)
    NameNode.stateChangeLog.debug("NameNode.invalidateCorruptReplicas: "
        + "invalidating corrupt replicas on " + nodes.size() + "nodes");
    for (String one : nodes) {
      DatanodeDescriptor node = datanodeMap.get(one);
      try {
        invalidateBlock(blk, node);
      } catch (IOException e) {
        NameNode.stateChangeLog.info("NameNode.invalidateCorruptReplicas "
            + "error in deleting bad block " + blk + " on " + node + e);
        gotException = true;
      }
      // Remove the block from corruptReplicasMap
      if (!gotException){ 
        try {
          corruptReplicas.removeFromCorruptReplicasMap(blk);
          } catch (IOException e) {
            LOG.error("invalidateCorruptReplicas: IOException occurs for block=" 
                + blk, e);
            continue;
            }
          }
      }     
  }
  
  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessReplicates() to
   * mark them in the excessReplicateMap.
   */
  private void processOverReplicatedBlock(Block block, short replication,
      DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {
    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeDescriptor> nonExcess = new ArrayList<DatanodeDescriptor>();
    BlockEntry be = null;
    try {
      be = stateManager.getStoredBlockBy(block.getBlockId());
    } catch (IOException e) {
      LOG.error("processOverReplicatedBlock: IOException occurs for block=" + block, e);
    }
    if (be == null) {
      return;
    }
    boolean exist = false;
    boolean corruptExist = false;
    Collection<Integer> dnids = be.getDatanodeIds();
    for (Iterator<Integer> it = dnids.iterator(); it.hasNext(); ) {
      DatanodeDescriptor cur = getDataNodeDescriptorByID(it.next());
      if (cur == null) {
        continue;
      }
      try {
        exist = excessReplicateMap.exist(cur.getStorageID(), block);
        corruptExist=corruptReplicas.isReplicaCorrupt(block, cur);
      } catch (IOException e) {
        LOG.error("processOverReplicatedBlock: IOException occurs for block=" + block +
            ", and datanodeId=" + cur, e);
        continue;
      }
      if (!exist) {
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
          if(!corruptExist) {
            nonExcess.add(cur);
          }
        }
      }
    }
    chooseExcessReplicates(nonExcess, block, replication, addedNode,
        delNodeHint);
  }
  
  /**
   * We want "replication" replicates for the block, but we now have too many.  
   * In this method, copy enough nodes from 'srcNodes' into 'dstNodes' such that:
   *
   * srcNodes.size() - dstNodes.size() == replication
   *
   * We pick node that make sure that replicas are spread across racks and
   * also try hard to pick one with least free space.
   * The algorithm is first to pick a node with least free space from nodes
   * that are on a rack holding more than one replicas of the block.
   * So removing such a replica won't remove a rack. 
   * If no such a node is available,
   * then pick a node with least free space
   */
  void chooseExcessReplicates(Collection<DatanodeDescriptor> nonExcess,
      Block b, short replication, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) {
    HashMap<String, ArrayList<DatanodeDescriptor>> rackMap = new HashMap<String, ArrayList<DatanodeDescriptor>>();
    for (Iterator<DatanodeDescriptor> iter = nonExcess.iterator(); iter.hasNext();) {
      DatanodeDescriptor node = iter.next();
      String rackName = node.getNetworkLocation();
      ArrayList<DatanodeDescriptor> datanodeList = rackMap.get(rackName);
      if (datanodeList == null) {
        datanodeList = new ArrayList<DatanodeDescriptor>();
      }
      datanodeList.add(node);
      rackMap.put(rackName, datanodeList);
    }

    // split nodes into two sets
    // priSet contains nodes on rack with more than one replica
    // remains contains the remaining nodes
    ArrayList<DatanodeDescriptor> priSet = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> remains = new ArrayList<DatanodeDescriptor>();
    for (Iterator<Entry<String, ArrayList<DatanodeDescriptor>>> iter = rackMap.entrySet().iterator(); iter.hasNext();) {
      Entry<String, ArrayList<DatanodeDescriptor>> rackEntry = iter.next();
      ArrayList<DatanodeDescriptor> datanodeList = rackEntry.getValue();
      if (datanodeList.size() == 1) {
        remains.add(datanodeList.get(0));
      } else {
        priSet.addAll(datanodeList);
      }
    }

    // pick one node to delete that favors the delete hint
    // otherwise pick one with least space from priSet if it is not empty
    // otherwise one node with least space from remains
    boolean firstOne = true;
    boolean putDone = false;
    while (nonExcess.size() - replication > 0) {
      DatanodeInfo cur = null;
      long minSpace = Long.MAX_VALUE;

      // check if we can del delNodeHint
      if (firstOne
          && delNodeHint != null
          && nonExcess.contains(delNodeHint)
          && (priSet.contains(delNodeHint) || (addedNode != null && !priSet.contains(addedNode)))) {
        cur = delNodeHint;
      } else { // regular excessive replica removal
        Iterator<DatanodeDescriptor> iter = priSet.isEmpty() ? remains.iterator() : priSet.iterator();
        while (iter.hasNext()) {
          DatanodeDescriptor node = iter.next();
          long free = node.getRemaining();

          if (minSpace > free) {
            minSpace = free;
            cur = node;
          }
        }
      }

      firstOne = false;
      // adjust rackmap, priSet, and remains
      String rack = cur.getNetworkLocation();
      ArrayList<DatanodeDescriptor> datanodes = rackMap.get(rack);
      datanodes.remove(cur);
      if (datanodes.isEmpty()) {
        rackMap.remove(rack);
      }
      if (priSet.remove(cur)) {
        if (datanodes.size() == 1) {
          priSet.remove(datanodes.get(0));
          remains.add(datanodes.get(0));
        }
      } else {
        remains.remove(cur);
      }

      nonExcess.remove(cur);
      
      try {
        putDone = excessReplicateMap.put(cur.getStorageID(), b);
      } catch (IOException e) {
        LOG.error("chooseExcessReplicates: can't put " + cur.getStorageID() 
            + " into excessReplicateMap");
        continue;
      }
      if(putDone){
        excessBlocksCount++;
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.chooseExcessReplicates: "
            + "(" + cur.getName() + ", " + b
            + ") is added to excessReplicateMap");
      }

      //
      // The 'excessblocks' tracks blocks until we get confirmation
      // that the datanode has deleted them; the only way we remove them
      // is when we get a "removeBlock" message.
      //
      // The 'invalidate' list is used to inform the datanode the block
      // should be deleted. Items are removed from the invalidate list
      // upon giving instructions to the namenode.
      //
      addToInvalidatesNoLog(b, cur);
      NameNode.stateChangeLog.info("BLOCK* NameSystem.chooseExcessReplicates: " + "("
              + cur.getName() + ", " + b
              + ") is added to recentInvalidateSets");
    }
  }
  
  /**
   * Modify (block-->datanode) map.  Remove block from set of 
   * needed replications if this takes care of the problem.
   * @return the block that is stored in blockMap.
   */
  Block addStoredBlock(Block block, DatanodeDescriptor node,
      DatanodeDescriptor delNodeHint, boolean isBlockReceiveCall)
      throws IOException {
    BlockEntry storedBlock = stateManager.getStoredBlockBy(block.getBlockId());
    if(storedBlock == null) {
      return rejectAddStoredBlock(block, node,
      "Block not in blockMap with any generation stamp");
    } else {
      if(storedBlock.getGenerationStamp() != block.getGenerationStamp()) {
        int fileid = storedBlock.getFileId();
        DbNodeFile file = null;
        try {
          file = fileManager.getFile(fileid);
        } catch (IOException ignored) { }
        if (file == null) {
          return rejectAddStoredBlock(block, node,
              "Block does not correspond to any file");
        }      

        boolean reportedOldGS = block.getGenerationStamp() < storedBlock.getGenerationStamp();
        boolean reportedNewGS = block.getGenerationStamp() > storedBlock.getGenerationStamp();
        boolean underConstruction = file.isUnderConstruction();
        boolean isLastBlock = file.getLastBlock() != null
            && file.getLastBlock().getBlockId() == block.getBlockId();

        // We can report a stale generation stamp for the last block under
        // construction,
        // we just need to make sure it ends up in targets.
        if (reportedOldGS && !(underConstruction && isLastBlock)) {
          return rejectAddStoredBlock(block, node,
              "Reported block has old generation stamp but is not the last block of "
                  + "an under-construction file. (current generation is "
                  + storedBlock.getGenerationStamp() + ")");
        }
        
        // Don't add blocks to the DN when they're part of the in-progress last
        // block
        // and have an inconsistent generation stamp. Instead just add them to
        // targets
        // for recovery purposes. They will get added to the node when
        // commitBlockSynchronization runs
        if (underConstruction && isLastBlock && (reportedOldGS || reportedNewGS)) {
          NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
              + "Targets updated: block " + block + " on " + node.getName()
              + " is added as a target for block " + storedBlock + " with size "
              + block.getNumBytes());
          try {
            ((DbNodePendingFile)file).addTarget(node);
          }catch(IOException ioe){
            LOG.warn("Pending file update exception. src=" + file.getFilePath() +
                ", block=" + block);
          }
          return block;
        }
      } // end of if genstamp check
    } // end of else

    int fileid = storedBlock.getFileId();
    DbNodeFile file = null;
    try {
      file = fileManager.getFile(fileid);
    } catch (IOException ignored) { }
    if (file == null) {
      return rejectAddStoredBlock(block, node,
          "Block does not correspond to any file");
    }

    assert storedBlock != null : "Block must be stored by now";

    // Is the block being reported the last block of an under-construction file?
    boolean blockUnderConstruction = false;
    boolean firstCopyOfTheLastBlock = false;
    if (file.isUnderConstruction()) {
      DbNodePendingFile pendingFile = (DbNodePendingFile)file;
      Block last = pendingFile.getLastBlock();
      if (last == null) {
        // This should never happen, but better to handle it properly
        // than to throw an NPE below.
        LOG.error("Null blocks for reported block=" + block + " stored="
            + storedBlock + " inode=" + pendingFile);
        return block;
      }
      Block b = new Block(storedBlock.getBlockId(), storedBlock.getNumbytes(),
          storedBlock.getGenerationStamp());
      blockUnderConstruction = last.equals(b);
      firstCopyOfTheLastBlock = 
        blockUnderConstruction && isBlockReceiveCall 
        && ((storedBlock.getDatanodeIds() == null) 
            ||(storedBlock.getDatanodeIds().size()==1 
                && storedBlock.getDatanodeIds().get(0) == -1));
      
    }
    
    boolean canAddToDb = true;
    if (isBlockReceiveCall) {
      if (block.getNumBytes() >= 0) {
        BlockInfo sblock = null;
        BlockInfo[] bi = file.getBlocks();
        if (bi != null) {
          for (int i = 0; i < bi.length; i++) {
            if (bi[i].getBlockId() == block.getBlockId()) {
              sblock = bi[i];
              break;
            }
          }
        }
        if(sblock == null) {
          // should NEVER be here....
          return rejectAddStoredBlock(block, node,
          "Cannot find the corresponding block id block = " + block); 
        }
        
        // add this node to the last block
        sblock.addNode(node);
        
        long cursize = storedBlock.getNumbytes();
        long rcvsize = block.getNumBytes();
        if (cursize == 0) {
          sblock.setNumBytes(rcvsize);
        } else if (cursize != rcvsize) {
          // skip the log info/warn messages
          try {
            if (cursize > rcvsize && !blockUnderConstruction) {
              markBlockAsCorrupt(block, node);
            } else {
              // new replica is larger in size than existing block.
              if (!blockUnderConstruction) {
                int numNodes = sblock.getDatanode().length;
                DatanodeDescriptor nodes[] = new DatanodeDescriptor[numNodes];
                int count = 0;
                for (int j = 0; j < numNodes; j++) {
                  DatanodeDescriptor dd = (DatanodeDescriptor) sblock
                      .getDatanode()[j];
                  if (!node.equals(dd)) {
                    nodes[count++] = dd;
                  }
                }
                for (int j = 0; j < count; j++) {
                  if (nodes[j] != null) {
                    markBlockAsCorrupt(block, nodes[j]);
                  }
                }
              }
              //
              // change the size of block in blocksMap
              //
              sblock.setNumBytes(rcvsize);
            }
          } catch (IOException e) {
            LOG.warn("Error in deleting bad block " + block + e);
          }
        }
        block = sblock;
      }
    } else { // for block report
      /*
       * here we cannot determine which one is corrupted and if 
       * we add this block to db, the numBytes inconsistency will
       * happen, so we will have to skip the add action.
       */
      if(storedBlock.getNumbytes() != block.getNumBytes()) {
        LOG.warn("block's numbytes are inconsistent for " + block + 
            ", skip to update the DB record");
        if(block.getNumBytes() == 0) {
          // the reported block should be corrupted
          markBlockAsCorrupt(block, node);
        }
        canAddToDb = false;
      }
    }

    boolean added = false;
    if (canAddToDb) {
      int dnid = node.getNodeid();
      try {
        if (dnid != -1) {
          /* add the blockid <-> dnid record to map table */
          LOG.info("BLOCK* STATUS of addStoredBlock: block=" + block
              + ", fileUnderConstruction=" + file.isUnderConstruction() + ", blockUnderConstruction = "
              + blockUnderConstruction + ", firstCopyOfTheLastBlock = " + firstCopyOfTheLastBlock);
          stateManager.receiveBlockFromDatanodeBy(dnid, block.getBlockId(),
              block.getNumBytes(), block.getGenerationStamp());
          added = true;
        }
      } catch (IOException ioe) {
        LOG.error("receiveBlockFromDatanodeBy failed", ioe);
        added = false;
      }
    }

    int curReplicaDelta = 0;

    if (added) {
      curReplicaDelta = 1;
      //
      // At startup time, because too many new blocks come in
      // they take up lots of space in the log file.
      // So, we log only when namenode is out of safemode.
      //
      if (!safeMode.get()) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
            + "blockMap updated: " + node.getName() + " is added to " + block
            + " size " + block.getNumBytes());
      }
    } else {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: "
          + "Redundant addStoredBlock request received for " + block + " on "
          + node.getName() + " size " + block.getNumBytes());
    }

    // filter out containingNodes that are marked for decommission.
    NumberReplicas num = countNodesInternal(block);
    int numLiveReplicas = num.liveReplicas();
    int numCurrentReplica = numLiveReplicas
        + pendingReplications.getNumReplicas(block);

    // check whether safe replication is reached for the block
    // incrementSafeBlockCount(numCurrentReplica);
    
    //
    // if file is being actively written to, then do not check
    // replication-factor here. It will be checked when the file is closed.
    //
    if (blockUnderConstruction) {
      /* here we check if exception occurs when add target
       * if no exception, follow the normal logic; otherwise,
       * it means ucFile may not exist due to other namenode's
       * operation, and we need to further check the replication. */
      try {
        // unlock early, the outer unlock will return false but
        // will not have negative impact...
        lockManager.unlockBlock(block.getBlockId());
        if (lockManager.lockFile(file.getFileId())) {
          try {
            if (fileManager.getPendingData(file.getFileId()) != null) {
              // still pending file
              DbNodePendingFile pendingFile = (DbNodePendingFile) file;
              if (firstCopyOfTheLastBlock) {
                /*
                 * we need to further check if all blocks has received. if some
                 * precedent blocks are not received when we call getBlocks, we
                 * SHOULD NOT update the length here
                 */
                boolean canUpdate = true;
                for (BlockInfo blkInfo : pendingFile.getBlocks()) {
                  if (blkInfo.getDatanode().length == 0) {
                    canUpdate = false;
                    break;
                  }
                }
                if (canUpdate) {
                  pendingFile.updateLength();
                }
              } // end of if firstCopyOfTheLastBlock
              pendingFile.addTarget(node);
            }
          } finally {
            lockManager.unlockFile(file.getFileId());
          }
        }
        return block;
      } catch (IOException ioe) {
        /*
         * If we meet IOException here, it's likely some other namenode has
         * completed corresponding file and addTarget operations failed
         */
        LOG.warn("Pending file update exception, "
            + "the pending file may be completed. src=" + file.getFilePath()
            + ", block=" + block);
        return block;
      }
    }

    // do not handle mis-replicated blocks during startup
    if (safeMode.get())
      return block;

      try {
        file = fileManager.getFile(fileid);
      } catch (IOException ignored) { }
      if (file == null) {
        return rejectAddStoredBlock(block, node,
            "Block does not correspond to any file");
      }
      // handle underReplication/overReplication
      short fileReplication = file.getReplication();
      if (numCurrentReplica >= fileReplication) {
        neededReplications.remove(block, numCurrentReplica,
            num.decommissionedReplicas, fileReplication);
      } else {
        updateNeededReplications(block, curReplicaDelta, 
            0, fileReplication);
      }
      if (numCurrentReplica > fileReplication) {
        processOverReplicatedBlock(block, fileReplication, node, delNodeHint);
      }
      // If the file replication has reached desired value
      // we can remove any corrupt replicas the block may have
      int corruptReplicasCount = corruptReplicas.numCorruptReplicas(block);
      int numCorruptNodes = num.corruptReplicas();
      if (numCorruptNodes != corruptReplicasCount) {
        LOG.warn("Inconsistent number of corrupt replicas for " + block
            + "blockMap has " + numCorruptNodes
            + " but corrupt replicas map has " + corruptReplicasCount);
      }
      if ((corruptReplicasCount > 0) && (numLiveReplicas >= fileReplication))
        invalidateCorruptReplicas(block);
    
    return block;
  }
  
  /**
   * The given node is reporting that it received a certain block.
   */
  public void blockReceived(DatanodeID nodeID,  
                                         Block block,
                                         String delHint
                                         ) throws IOException {
    NameNode.stateChangeLog.info("*BLOCK* blockReceived from " + 
        nodeID + ", block=" + block);
    DatanodeDescriptor node = getDatanode(nodeID);
    if (node == null) {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceived: "
                                   + block + " is received from an unrecorded node " 
                                   + nodeID.getName());
      throw new IllegalArgumentException(
                                         "Unexpected exception.  Got blockReceived message from node " 
                                         + block + ", but there is no info for it");
    }
    
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.blockReceived: "
                                    +block+" is received from " + nodeID.getName());
    }
    
    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(node)) {
      setDatanodeDead(node);
      throw new DisallowedDatanodeException(node);
    }
    
    // decrement number of blocks scheduled to this datanode.
    node.decBlocksScheduled();
    
    // get the deletion hint node
    DatanodeDescriptor delHintNode = null;
    if(delHint!=null && delHint.length()!=0) {
      delHintNode = datanodeMap.get(delHint);
      if(delHintNode == null) {
        NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceived: "
            + block
            + " is expected to be removed from an unrecorded node " 
            + delHint);
      }
    }
    
    //
    // Modify the blocks->datanode map and node's map.
    // 
    pendingReplications.remove(block);
    if(lockManager.lockBlock(block.getBlockId())) {
      try {
        addStoredBlock(block, node, delHintNode, true);
      } finally {
        lockManager.unlockBlock(block.getBlockId());
      }
    } else {
      throw new IOException("cannot get block lock for " + block);
    }
  }
  
  /**
   * It will update the targets for INodeFileUnderConstruction
   * 
   * @param nodeID
   *          - DataNode ID
   * @param blocksBeingWritten
   *          - list of blocks which are still inprogress.
   * @throws IOException
   */
  public void processBlocksBeingWrittenReport(DatanodeID nodeID,
      BlockListAsLongs blocksBeingWritten) throws IOException {
    DatanodeDescriptor dataNode = getDatanode(nodeID);
    if (dataNode == null) {
      throw new IOException("ProcessReport from unregistered node: "
          + nodeID.getName());
    }

    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(dataNode)) {
      setDatanodeDead(dataNode);
      throw new DisallowedDatanodeException(dataNode);
    }

    Block block = new Block();

    for (int i = 0; i < blocksBeingWritten.getNumberOfBlocks(); i++) {
      block.set(blocksBeingWritten.getBlockId(i), blocksBeingWritten
          .getBlockLen(i), blocksBeingWritten.getBlockGenStamp(i));
      
      BlockEntry storedBlock = stateManager.getStoredBlockBy(block.getBlockId());

      if (storedBlock == null) {
        rejectAddStoredBlock(block, dataNode,
            "Block not in blockMap with any generation stamp");
        continue;
      }
      
      int fid = storedBlock.getFileId();
      long blkId = storedBlock.getBlockId();
      
      lockManager.lockFileBlock(fid, blkId);
      try {
        DbNodeFile file = fileManager.getFile(fid);
        if (file == null) {
          rejectAddStoredBlock(block, dataNode,
              "Block does not correspond to any file");
          continue;
        }
  
        boolean underConstruction = file.isUnderConstruction();
        boolean isLastBlock = file.getLastBlock() != null
            && file.getLastBlock().getBlockId() == block.getBlockId();
  
        // Must be the last block of a file under construction,
        if (!underConstruction) {
          rejectAddStoredBlock(block, dataNode,
              "Reported as block being written but is a block of closed file.");
          continue;
        }
  
        if (!isLastBlock) {
          rejectAddStoredBlock(block, dataNode,
              "Reported as block being written but not the last block of "
                  + "an under-construction file.");
          continue;
        }
  
        DbNodePendingFile pendingFile = (DbNodePendingFile)file;
        pendingFile.addTarget(dataNode);
      } finally {
        lockManager.unlockFileBlock(fid, blkId);
      }
    }
  }
  
  /**
   * The given node is reporting all its blocks.  Use this info to 
   * update the (machine-->blocklist) and (block-->machinelist) tables.
   */
  public void processReport(final DatanodeID nodeID, 
                            final BlockListAsLongs newReport) throws IOException {
    long startTime = now();
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: "
                             + "from " + nodeID.getName()+" " + 
                             newReport.getNumberOfBlocks()+" blocks");
    }
    final DatanodeDescriptor node = getDatanode(nodeID);
    if (node == null) {
      throw new IOException("ProcessReport from unregisterted node: "
                            + nodeID.getName());
    }
    
    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(node)) {
      setDatanodeDead(node);
      throw new DisallowedDatanodeException(node);
    }
    
    int nodeid = node.getNodeid();
    if( nodeid == -1){
      throw new IOException("can't find the datanode id of " + node);
    }
    
    //
    // Modify the (block-->datanode) map, according to the difference
    // between the old and new block report.
    //
    final Collection<Block> toAdd = new LinkedList<Block>();
    final Collection<Block> toRemove = new LinkedList<Block>();
    final Collection<Block> toInvalidate = new LinkedList<Block>();
    
    
    // we use a separate thread to asynchronously 
    // process time-consuming task
    try {
      asynchronousProcessor.execute(new Runnable() {
        @Override
        public void run() {
          long finReportDiffTime;
          long finToRemoveTime;
          long finToAddTime;
          long finToInvalidateTime;
          long startTime = now();
          // STEP 1: get diff report
          try {
            node.reportDiff(stateManager, fileManager, 
                newReport, toAdd, toRemove, toInvalidate);
          } catch (IOException ioe) {
            LOG.error("processReport: reportDiff for node=" 
                + node.getNodeid(), ioe);
          } finally {
            finReportDiffTime = now();
          }
          // STEP 2: handle toRemove blocks
          for (Block rblk : toRemove) {
            try {
              if (lockManager.lockBlock(rblk.getBlockId())) {
                try {
                  removeStoredBlock(rblk, node);
                } finally {
                  lockManager.unlockBlock(rblk.getBlockId());
                }
              } else {
                LOG.error("failed to get block lock for block=" + rblk);
              }
            } catch (IOException ioe) {
              LOG.error("processReport: removeStoredBlock for block=" + rblk, ioe);
            }
          }
          finToRemoveTime = now();
          // STEP 3: handle toAdd blocks
          for (Block ablk : toAdd) {
            try {
              if (lockManager.lockBlock(ablk.getBlockId())) {
                try {
                  addStoredBlock(ablk, node, null, false);
                } finally {
                  lockManager.unlockBlock(ablk.getBlockId());
                }
              } else {
                LOG.error("failed to get block lock for block=" + ablk);
              }
            } catch (IOException ioe) {
              LOG.error("processReport: addStoredBlock for block=" + ablk, ioe);
            }
          }
          finToAddTime = now();
          // STEP4: handle invalidate blocks
          for (Block iblk : toInvalidate) {
            if (NameNode.stateChangeLog.isDebugEnabled()) {
              NameNode.stateChangeLog.debug("*BLOCK* NameSystem.processReport: block "
                  + iblk + " on " + node.getName() + " size " + iblk.getNumBytes()
                  + " does not belong to any file.");
            }
            addToInvalidates(iblk, node);
          }
          finToInvalidateTime = now();
          
          long elapsed = finToInvalidateTime-startTime;
          NameNode.getNameNodeMetrics().blockReport.inc(elapsed);
          
          if(NameNode.stateChangeLog.isInfoEnabled()) {
            NameNode.stateChangeLog.info("*BLOCK* NameNode.blockReport: processing" +
            		" blockReport for node=" + node.getName() + 
            		" completed, elapsed(msec)=" + (finToInvalidateTime-startTime) +
            		" [reportdiff=" + (finReportDiffTime-startTime) +
            		", removeblocks=" + (finToRemoveTime-finReportDiffTime) +
            		", addblocks=" + (finToAddTime-finToRemoveTime) +
            		", invalidateblocks=" + (finToInvalidateTime-finToAddTime) + "]");
          }
        }
      });
    }catch(RejectedExecutionException ree) {
      LOG.error("blockReportProcessor refused to add more tasks", ree);
    }
    
    long elapsed = now() - startTime;
    if(NameNode.stateChangeLog.isInfoEnabled()) {
      NameNode.stateChangeLog.info("*BLOCK* NameNode.blockReport: asychronous call completed, elapsed=" 
          + elapsed + "(msec) for Datanode=" + node.getName());
    } 
  }
  
  /**
   * The given node has reported in.  This method should:
   * 1) Record the heartbeat, so the datanode isn't timed out
   * 2) Adjust usage stats for future block allocation
   * 
   * If a substantial amount of time passed since the last datanode 
   * heartbeat then request an immediate block report.  
   * 
   * @return an array of datanode commands 
   * @throws IOException
   */
  DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
      long capacity, long dfsUsed, long remaining,
      int xceiverCount, int xmitsInProgress) throws IOException {
    
    DatanodeCommand cmd = null;
    boolean newOne = false;
    synchronized (datanodeMap) {
      DatanodeDescriptor nodeinfo = null;
      try {
        nodeinfo = getDatanode(nodeReg);
      } catch(UnregisteredDatanodeException e) {
        return new DatanodeCommand[] {DatanodeCommand.REGISTER};
      }
      
      // Check if this datanode should actually be shutdown instead. 
      if (nodeinfo != null && shouldNodeShutdown(nodeinfo)) {
        setDatanodeDead(nodeinfo);
        throw new DisallowedDatanodeException(nodeinfo);
      }
      
      if (nodeinfo == null || !nodeinfo.isAlive) {
        LOG.warn(nodeinfo == null ? "nodeinfo is null" : "nodeinfo.isAlive=" +nodeinfo.isAlive 
            + ", should be register...");
        return new DatanodeCommand[]{DatanodeCommand.REGISTER};
      }
      
      synchronized(heartbeats) {
        updateStats(nodeinfo, false);
        nodeinfo.updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount);
        updateStats(nodeinfo, true);
        if (!heartbeats.contains(nodeinfo)) {
          heartbeats.add(nodeinfo);
          newOne = true;
        }
      }
      // out of heartbeats sync
      if(newOne) {
        boolean res = stateManager.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining, xceiverCount, now(), DatanodeInfo.AdminStates.NORMAL);
        if(res){
          LOG.info("Heartbeat sync to bm " + nodeReg + " successed");
        } else {
          LOG.info("Heartbeat sync to bm " + nodeReg + " failed");
        }
        externalDatanodeCommandsHandler.removeIfExists(nodeinfo);
      }
      // check lease recovery
      cmd = nodeinfo.getLeaseRecoveryCommand(Integer.MAX_VALUE);
      if (cmd != null) {
        return new DatanodeCommand[] {cmd};
      }
      
      ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>(2);
      //check pending replication
      cmd = nodeinfo.getReplicationCommand(
            maxReplicationStreams - xmitsInProgress);
      if (cmd != null) {
        cmds.add(cmd);
      }
      //check block invalidation
      cmd = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
      if (cmd != null) {
        cmds.add(cmd);
      }
      if (!cmds.isEmpty()) {
        return cmds.toArray(new DatanodeCommand[cmds.size()]);
      }
    }
    
    return null;
  }
  
  private void updateStats(DatanodeDescriptor node, boolean isAdded) {
    //
    // The statistics are protected by the heartbeat lock
    //
    assert(Thread.holdsLock(heartbeats));
    if (isAdded) {
      capacityTotal += node.getCapacity();
      capacityUsed += node.getDfsUsed();
      capacityRemaining += node.getRemaining();
      totalLoad += node.getXceiverCount();
    } else {
      capacityTotal -= node.getCapacity();
      capacityUsed -= node.getDfsUsed();
      capacityRemaining -= node.getRemaining();
      totalLoad -= node.getXceiverCount();
    }
  }
  /**
   * Checks if the Admin state bit is DECOMMISSIONED.  If so, then 
   * we should shut it down. 
   * 
   * Returns true if the node should be shutdown.
   */
  private boolean shouldNodeShutdown(DatanodeDescriptor node) {
    return (node.isDecommissioned());
  }
  
  private void setDatanodeDead(DatanodeDescriptor node) throws IOException {
    node.setLastUpdate(0);
  }
  
  public InetSocketAddress getDFSNameNodeAddress() {
    return nameNodeAddress;
  }
  /**
   * Get data node by storage ID.
   * 
   * @param nodeID
   * @return DatanodeDescriptor or null if the node is not found.
   * @throws IOException
   */
  public DatanodeDescriptor getDatanode(DatanodeID nodeID) throws IOException {
    UnregisteredDatanodeException e = null;
    synchronized(datanodeMap) {
      DatanodeDescriptor node = datanodeMap.get(nodeID.getStorageID());
      if (node == null) {
        node = getDataNodeDescriptorByID(nodeID.getNodeid());
        if(node == null) {
  	      LOG.warn("Cannot find the corresponding " +
  	          "DatanodeDescriptor using data node storageID:" + nodeID.getStorageID());
  	      return null;
        }
      }
      if (!node.getName().equals(nodeID.getName())) {
        e = new UnregisteredDatanodeException(nodeID, node);
        NameNode.stateChangeLog.fatal("BLOCK* NameSystem.getDatanode: "
                                      + e.getLocalizedMessage());
        throw e;
      }
      return node;
    }
  }
  
  private boolean inHostsList(DatanodeID node, String ipAddr) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() || 
            (ipAddr != null && hostsList.contains(ipAddr)) ||
            hostsList.contains(node.getHost()) ||
            hostsList.contains(node.getName()) || 
            ((node instanceof DatanodeInfo) && 
             hostsList.contains(((DatanodeInfo)node).getHostName())));
  }

  private boolean inExcludedHostsList(DatanodeID node, String ipAddr) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return ((ipAddr != null && excludeList.contains(ipAddr))
        || excludeList.contains(node.getHost())
        || excludeList.contains(node.getName()) || ((node instanceof DatanodeInfo) && excludeList
        .contains(((DatanodeInfo) node).getHostName())));
  }
  
  /**
   * Return true if there are any blocks on this node that have not
   * yet reached their replication factor. Otherwise returns false.
   */
  private boolean isReplicationInProgress(DatanodeDescriptor srcNode) {
    boolean status = false;
    try {
      Collection<BlockEntry> dnBlockIDList = stateManager
          .getAllBlocksOnDatanode(srcNode.getNodeid());
      for (final Iterator<BlockEntry> i = 
        dnBlockIDList.iterator(); i.hasNext();) {
        final BlockEntry be = i.next();
        Block block = new Block(be.getBlockId(), be.getNumbytes(), be
            .getGenerationStamp());
        FileStatus fstatus = null;
        try {
          fstatus = fileManager.getFileStatus(be.getFileId());
        } catch (IOException e) {
          fstatus = null;
        }

        if (fstatus != null) {
          try {
            NumberReplicas num = countNodes(block);
            int curReplicas = num.liveReplicas();
            int curExpectedReplicas = fstatus.getReplication();
            if (curExpectedReplicas > curReplicas) {
              status = true;
              if (!neededReplications.contains(block)
                  && pendingReplications.getNumReplicas(block) == 0) {
                //
                // These blocks have been reported from the datanode
                // after the startDecommission method has been executed. These
                // blocks were in flight when the decommission was started.
                //
                neededReplications.add(block, curReplicas, num
                    .decommissionedReplicas(), curExpectedReplicas);
              }
            }
          } catch (IOException ioe) {
            LOG.error("isReplicationInProgress: block=" + block, ioe);
          }
        }
      } // end of loop
      return status;
    } catch (IOException e) {
      LOG.error("isReplicationInProgress failed", e);
      return false;
    }
  }
  
  /**
   * Change, if appropriate, the admin state of a datanode to 
   * decommission completed. Return true if decommission is complete.
   */
  boolean checkDecommissionStateInternal(DatanodeDescriptor node) {
    //
    // Check to see if all blocks in this decommissioned
    // node has reached their target replication factor.
    //
    if (node.isDecommissionInProgress()) {
      if (!isReplicationInProgress(node)) {
        node.setDecommissioned();
        LOG.info("Decommission complete for node " + node.getName());
      }
    }
    if (node.isDecommissioned()) {
      return true;
    }
    return false;
  }
  
  private boolean verifyNodeRegistration(
      DatanodeRegistration nodeReg, String ipAddr) throws IOException {
    if (!inHostsList(nodeReg, ipAddr)) {
      return false;
    }
    if (inExcludedHostsList(nodeReg, ipAddr)) {
      DatanodeDescriptor node = getDatanode(nodeReg);
      if (node == null) {
        throw new IOException("verifyNodeRegistration: unknown datanode "
            + nodeReg.getName());
      }
      
      if (!checkDecommissionStateInternal(node)) {
        startDecommission(node);
      }
    }
    return true;
  }
  
  /**
   * Register Datanode.
   * <p>
   * The purpose of registration is to identify whether the new datanode
   * serves a new data storage, and will report new data block copies,
   * which the namenode was not aware of; or the datanode is a replacement
   * node for the data storage that was previously served by a different
   * or the same (in terms of host:port) datanode.
   * The data storages are distinguished by their storageIDs. When a new
   * data storage is reported the namenode issues a new unique storageID.
   * <p>
   * Finally, the namenode returns its namespaceID as the registrationID
   * for the datanodes. 
   * namespaceID is a persistent attribute of the name space.
   * The registrationID is checked every time the datanode is communicating
   * with the namenode. 
   * Datanodes with inappropriate registrationID are rejected.
   * If the namenode stops, and then restarts it can restore its 
   * namespaceID and will continue serving the datanodes that has previously
   * registered with the namenode without restarting the whole cluster.
   * 
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode#register()
   */
  public void registerDatanode(DatanodeRegistration nodeReg
                                            ) throws IOException {
    String dnAddress = Server.getRemoteAddress(); // ip address
    if (dnAddress == null) {
      // Mostly called inside an RPC.
      // But if not, use address passed by the data-node.
      dnAddress = nodeReg.getHost();
    }      

    // check if the datanode is allowed to be connect to the namenode
    if (!verifyNodeRegistration(nodeReg, dnAddress)) {
      throw new DisallowedDatanodeException(nodeReg);
    }
    
    String hostName = nodeReg.getHost();

    DatanodeID dnReg = null;
    if (!"127.0.0.1".equals(dnAddress)){
      // I'm running in a real cluster, update the datanode's name with ip:port
      dnReg = new DatanodeID(dnAddress + ":" + nodeReg.getPort(),
                                        nodeReg.getStorageID(),
                                        nodeReg.getInfoPort(),
                                        nodeReg.getIpcPort());
    } else {
      // this is mainly for unit test
      dnReg = new DatanodeID(hostName + ":" + nodeReg.getPort(),
                              nodeReg.getStorageID(),
                              nodeReg.getInfoPort(),
                              nodeReg.getIpcPort());
    }
    nodeReg.updateRegInfo(dnReg);
    
    NameNode.stateChangeLog.info(
        "BLOCK* NameSystem.registerDatanode: "
        + "node registration from " + nodeReg.getName()
        + " storage " + nodeReg.getStorageID());
    
    int res = stateManager.registerDatanodeBy(nodeReg, hostName, now());
    if(res == 0){
      LOG.info("register datanode " + nodeReg + " to blocksmap service successfully");
      stateManager.handleHeartbeat(nodeReg, 0, 0, 0, 0, now(), DatanodeInfo.AdminStates.NORMAL);
    } else if (res == 1){
      LOG.info("datanode registered before,just update");
      stateManager.handleHeartbeat(nodeReg, 0, 0, 0, 0, now(), DatanodeInfo.AdminStates.NORMAL);
    } else {
      throw new IOException("register datanode to blocksmap failed");
    }
    updateDatanodesMap(true);
  }
  
  /* Resolve a node's network location */
  private void resolveNetworkLocation (DatanodeDescriptor node) {
    List<String> names = new ArrayList<String>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      // get the node's IP address
      names.add(node.getHost());
    } else {
      // get the node's host name
      String hostName = node.getHostName();
      int colon = hostName.indexOf(":");
      hostName = (colon==-1)?hostName:hostName.substring(0,colon);
      names.add(hostName);
    }
    
    // resolve its network location
    List<String> rName = dnsToSwitchMapping.resolve(names);
    String networkLocation;
    if (rName == null) {
      LOG.error("The resolve call returned null! Using " + 
          NetworkTopology.DEFAULT_RACK + " for host " + names);
      networkLocation = NetworkTopology.DEFAULT_RACK;
    } else {
      networkLocation = rName.get(0);
    }
    node.setNetworkLocation(networkLocation);
  }
  
  /**
   * remove a datanode descriptor
   * @param nodeID datanode ID
   */
  public void removeDatanode(DatanodeID nodeID) 
    throws IOException {
    DatanodeDescriptor nodeInfo = getDatanode(nodeID);
    if (nodeInfo != null) {
      removeDatanode(nodeInfo);
    } else {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.removeDatanode: "
                                   + nodeID.getName() + " does not exist");
    }
  }
  
  /**
   * remove a datanode descriptor
   * @param nodeInfo datanode descriptor
   */
  private void removeDatanode(DatanodeDescriptor nodeInfo) {    
    removeDatanodeInternal(nodeInfo);
    removeBlocksOnDatanode(nodeInfo);
  }
  
  void removeDatanodeInternal(DatanodeDescriptor nodeDescr) {
    boolean containted = false;
    synchronized (datanodeMap) {
      synchronized (heartbeats) {
        if (nodeDescr.isAlive) {
          updateStats(nodeDescr, false);
          if ((containted = heartbeats.contains(nodeDescr))) {
            heartbeats.remove(nodeDescr);
          }
          nodeDescr.isAlive = false;
        }
      }
      synchronized (clusterMap) {
        clusterMap.remove(nodeDescr);
      }
      nodeDescr.resetBlocks();
      //wipe out datanode from datanodemap
      datanodeMap.remove(nodeDescr.getStorageID());
      host2DataNodeMap.remove(nodeDescr);
    } // end of datanodeMap synchronization
    NameNode.stateChangeLog.info(
        "BLOCK* NameSystem.wipeDatanode: "
        + nodeDescr.getName() + " storage " + nodeDescr 
        + " is removed from datanodeMap.");
    
    removeFromInvalidates(nodeDescr.getStorageID());
    if (!containted) {
      externalDatanodeCommandsHandler.removeIfExists(nodeDescr);
    }
  }
  
  void removeBlocksOnDatanode(DatanodeDescriptor nodeInfo) {
    /* add a distributed lock to secure only one namenode can execute
     * 'real' removeStoredBlock operations */
    Collection<BlockEntry> allBlocksOnDn = null;
    try {
      if(lockManager.trylockDataNode(nodeInfo.getNodeid())) {
        try {
          allBlocksOnDn = stateManager.getAllBlocksOnDatanode(nodeInfo.getNodeid());
          if (allBlocksOnDn != null && allBlocksOnDn.size() > 0) {
            LOG.info("I am responsible for deleting all blocks in datanode " +
                nodeInfo.getStorageID() + " from blockmap.");
              for (BlockEntry entry : allBlocksOnDn) {
                Block b = new Block();
                b.setBlockId(entry.getBlockId());
                b.setNumBytes(entry.getNumbytes());
                b.setGenerationStamp(entry.getGenerationStamp());
                try {
                  if(lockManager.lockBlock(b.getBlockId())) {
                    try {
                      removeStoredBlock(b, nodeInfo);
                    } finally {
                      lockManager.unlockBlock(b.getBlockId());
                    }
                  } else {
                    LOG.error("cannot get block lock for block=" + b);
                  }
                } catch(IOException ioe) {
                  LOG.error("removeDatanode: IOException occurs for nodeId=" + nodeInfo +
                      "block=" + b, ioe);
                }
              }
          } else {
            LOG.info("the blocks of datanode " + nodeInfo.getStorageID() 
                + " has been deleted from blockmap by another namenode.");
          }
        } finally {
          lockManager.unlockDataNode(nodeInfo.getNodeid());
        }
      }
    } catch (IOException e) {
      LOG.error("removeDatanode: IOException occurs for nodeId=" + nodeInfo, e);
      return;
    }
  }
  
  /**
   * Remove a datanode from the invalidatesSet
   * @param n datanode
   */
  void removeFromInvalidates(String storageID) {
    try {
      int blockCount = recentInvalidateSets.remove(storageID);
      if (blockCount > 0)
        pendingDeletionBlocksCount -= blockCount;
    } catch (IOException e) {
      LOG.error("removeFromInvalidates: IOException occurs for datanode=" + storageID, e);
    }
  }
  
  /**
   * Physically remove node from datanodeMap.
   * 
   * @param nodeID node
   */
  void wipeDatanode(DatanodeID nodeID) {
    String key = nodeID.getStorageID();
    DatanodeDescriptor dnDesc = null;
    synchronized(datanodeMap) {
      dnDesc = datanodeMap.remove(key);
    }
    if(dnDesc != null) {
      host2DataNodeMap.remove(dnDesc);
    }
    NameNode.stateChangeLog.info(
                                  "BLOCK* NameSystem.wipeDatanode: "
                                  + nodeID.getName() + " storage " + key 
                                  + " is removed from datanodeMap.");
  }
  
  private boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }
  
  /**
   * stores the modification and access time for this inode. 
   * The access time is precise upto an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    if (!isAccessTimeSupported() && atime != -1) {
      throw new IOException("Access time for hdfs is not configured. " +
                            " Please set dfs.support.accessTime configuration parameter.");
    }
    if (safeMode.get()) {
      throw new SafeModeException("Cannot set accesstimes  for " + src);
    }
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    if (nodeInfo != null) {
      if (lockManager.lockFile(nodeInfo.fid)) {
        try {
          stateManager.setTimes(src, mtime, atime);
        } finally {
          lockManager.unlockFile(nodeInfo.fid);
        }
      } else {
        throw new IOException("Cannot get file lock for " + src);
      }
      FileStatus status = fileManager.getFileStatus(nodeInfo.fid);
      if (status != null) {
        if (auditLog.isInfoEnabled()) {
          logAuditEvent(UserGroupInformation.getCurrentUGI(), Server
              .getRemoteIp(), "setTimes", src, null, status);
        }
      }
    } else {
      throw new FileNotFoundException("File " + src + " does not exist.");
    }
  }
  
  /** Persist all metadata about this file.
   * @param src The string representation of the path
   * @param clientName The string representation of the client
   * @throws IOException if path does not exist
   */
  void fsync(String src, String clientName) throws IOException {
    // in adfs, we only check lease
    NameNode.stateChangeLog.info("BLOCK* NameSystem.fsync: file " + src
        + " for " + clientName);
    TimeTrackerAPI at = 
      TimeTrackerFactory.creatTimeTracker("fsync(" +src + ")", 
          enableTimeTracker);
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    at.set("getDbNodeInfo");
    try {
      if(nodeInfo != null) {
        checkLease(nodeInfo, clientName);
        at.set("checkLease");
      } else {
        throw new FileNotFoundException("fsync: src=" + src + " doesn't exist!");
      }
    } finally {
      System.out.println(at.close());
    }
  }
  
  /**
   * Set the namespace quota and diskspace quota for a directory.
   * See {@link ClientProtocol#setQuota(String, long, long)} for the 
   * contract.
   */
  void setQuota(String path, long nsQuota, long dsQuota) throws IOException {
    // TODO
  }
  
  ContentSummary getContentSummary(String src) throws IOException {
    LOG.info("getContentSummary from DFSClient called"); 
    return fileManager.getContentSummary(src);
  }
  
  /** Is this name system running? */
  boolean isRunning() {
    return fsRunning;
  }
  
  /**
   * Dump all metadata into specified file
   */
  void metaSave(String filename) throws IOException {
    // TODO
  }
  
  void finalizeUpgrade() throws IOException {
    // TODO
  }
  
  UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action 
  ) throws IOException {
    // TODO
    return null;
  }
  
  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists.  It
   * checks if any of the hosts have changed states:
   * 1. Added to hosts  --> no further work needed here.
   * 2. Removed from hosts --> mark AdminState as decommissioned. 
   * 3. Added to exclude --> start decommission.
   * 4. Removed from exclude --> stop decommission.
   */
  public void refreshNodes(Configuration conf) throws IOException {
    // BLOCK_INVALIDATE_CHUNnamesystem.refreshNodes(new Configuration());
    if (conf == null) {
      conf = new Configuration();
    }
    hostsReader.updateFileNames(conf.get("dfs.hosts", ""), conf.get("dfs.hosts.exclude", ""));
    hostsReader.refresh();
    LOG.info("Nodes refreshed");
    synchronized (datanodeMap) {
      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it
          .hasNext();) {
        DatanodeDescriptor node = it.next();
        // Check if not include.
        if (!inHostsList(node, null)) {
          node.setDecommissioned(); // case 2.
        } else {
          if (inExcludedHostsList(node, null)) {
            if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
              startDecommission(node); // case 3.
            }
          } else {
            if (node.isDecommissionInProgress() || node.isDecommissioned()) {
              stopDecommission(node); // case 4.
            }
          }
        }
      }
    } // synchronized datanodemap
  }
  
  /**
   * Save namespace image.
   * This will save current namespace into fsimage file and empty edits file.
   * Requires superuser privilege and safe mode.
   * 
   * @throws AccessControlException if superuser privilege is violated.
   * @throws IOException if 
   */
  void saveNamespace() throws AccessControlException, IOException {
    throw new UnsupportedOperationException("saveNamespace");
  }
  
  /**
   * Get a listing of all files at 'src'.  The Object[] array
   * exists so we can return file attributes (soon to be implemented)
   */
  public FileStatus[] getListing(String src) throws IOException {
    FileStatus[] files = StateManager.fileArrayToFileStatusArray(
        stateManager.getListing(src));
    return files;
  }
  
  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder) throws IOException {
    leaseManager.renewLease(holder);
  }
  
  /**
   * Create all the necessary directories
   */
  public boolean mkdirs(String src, FsPermission masked
      ) throws IOException {
    if (safeMode.get()) {
      throw new SafeModeException("Cannot mkdir " + src);
    }
    boolean ret = stateManager.mkdirs(src);
    if(ret) {
      if (auditLog.isInfoEnabled()) {
        logAuditEvent(UserGroupInformation.getCurrentUGI(), 
            Server.getRemoteIp(), "mkdirs", src, null, null);
      }
    }
    return ret;
  }
  
  /**
   * Remove the indicated filename from namespace. If the filename 
   * is a directory (non empty) and recursive is set to false then throw exception.
   */
  public boolean delete(String src, boolean recursive) throws IOException {
    if (safeMode.get()) {
      throw new SafeModeException("Cannot delete " + src);
    }
    boolean ret = deleteInternal(src, recursive);
    if(ret) {
      if (auditLog.isInfoEnabled()) {
        logAuditEvent(UserGroupInformation.getCurrentUGI(), Server.getRemoteIp(), 
            (recursive?"delete recursively":"delete"), src, null, null);
      }
    }
    return ret;
  }
  
  boolean deleteInternal(String src, final boolean recursive) throws IOException {
    // check src is existed in namespace
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    if(nodeInfo == null){
      NameNode.stateChangeLog.info("DIR* FSNamesystem.delete: " + 
          " failed to remove " + src + " because it does not exist");
      return false;
    } 
    FileStatus status = nodeInfo.fs;
    // check dir and recursive recursive
    if ((!recursive) && (status.isDir())) {
	    FileStatus[] children = getListing(src);
	    if(children != null && children.length > 0 )
	      throw new IOException(src + " is not empty");
    }
    
    com.taobao.adfs.file.File[] files = 
      stateManager.getDescendant(src, true, true);
    
    if(files == null || files.length < 1) {
      return true;
    }
    
    // just move to trash bin and do some light load ops
    // instead of time-consuming deletion in db
    String trashPath = StateManager.getTrashPath(files[0]);
    stateManager.rename(src, trashPath);
    for(int i = 0; i < files.length; i++) {
      if(files[i] == null || files[i].isDir())
        continue;
      int fileId = files[i].id;
      leaseManager.removeLease(fileId);
      fileManager.removePendingFile(fileId);
    }

    return true;
  }
  
  /** Change the indicated filename. */
  public boolean renameTo(String src, String dst) throws IOException {
    if (safeMode.get()) {
      throw new SafeModeException("Cannot renameTo " + src);
    }
    boolean ret = renameToInternal(src, dst);
    if(ret) {
      logAuditEvent(UserGroupInformation.getCurrentUGI(), 
          Server.getRemoteIp(), "rename", src, dst, null);
    }
    return ret;
  }
  
  private boolean renameToInternal(String src, String dst) throws IOException {
    // check the validation of the source
    if (stateManager.getFileInfo(src) == null) {
      NameNode.stateChangeLog.warn("DIR* FSNamesystem.renameTo: "
          + " failed to rename " + src + " to " + dst
          + " because source does not exist");
      return false;
    }

    boolean changed = false;
    FileStatus dstStatus = 
      StateManager.fileToFileStatus(stateManager.getFileInfo(dst));
    if (dstStatus != null && dstStatus.isDir()) {
      dst += Path.SEPARATOR + new Path(src).getName();
      changed = true;
    }

    // check the validity of the destination
    if (dst.equals(src)) {
      return true;
    }

    // dst cannot be a sub directory or a file under src
    if (dst.startsWith(src) && dst.charAt(src.length()) == Path.SEPARATOR_CHAR) {
      NameNode.stateChangeLog.warn("DIR* FSNamesystem.renameTo :"
          + " failed to rename " + src + " to " + dst
          + " because destination starts with src");
      return false;
    }

    if (changed) { // only changed need to recheck
      dstStatus = 
        StateManager.fileToFileStatus(stateManager.getFileInfo(dst));
    }
    if (dstStatus != null) {
      NameNode.stateChangeLog.warn("DIR* FSNamesystem.renameTo: "
          + "failed to rename " + src + " to " + dst
          + " because destination exists");
      return false;
    }

    boolean res = false;
    try {
      stateManager.rename(src, dst);
      res = true;
    } catch (IOException ioe) {
      NameNode.stateChangeLog.error("DIR* FSNamesystem.renameTo: " + src
          + " failed to renamed to " + dst, ioe);
    }
    return res;
  }
  
  /**
   * 
   * @param pendingFile the file to be finalized
   * @param synCount if we check block replication factor synchronously
   * @throws IOException
   */
  private void finalizeDbNodePendingFileUnder(final DbNodePendingFile pendingFile) throws IOException {
    fileManager.convertToDbNodeFile(pendingFile);
    leaseManager.removeLease(pendingFile.getClientName(), pendingFile.getFileId());
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("Removed lease on file " + pendingFile
          + " from client " + pendingFile.getClientName());
    }
    // to improve performance, we choose to 
    // asynchronously check replication factor
    try {
      asynchronousProcessor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            if(lockManager.lockFile(pendingFile.getFileId())){
              try{
                checkReplicationFactor(pendingFile);
              } finally {
                lockManager.unlockFile(pendingFile.getFileId());
              }
            }
          } catch (IOException ioe) {
            LOG.error("checkReplicationFactor for file=" + pendingFile, ioe);
          }
        } // end of run
      });
    } catch (RejectedExecutionException ree) {
      LOG.error("checkReplicationFactor for file=" + pendingFile, ree);
    }
  }
  
  /** 
   * Check all blocks of a file. If any blocks are lower than their intended
   * replication factor, then insert them into neededReplication
   * 
   */
  private void checkReplicationFactor(DbNodeFile file) throws IOException {
    int numExpectedReplicas = file.getReplication();
    Block[] pendingBlocks = file.getBlocks();
    int nrBlocks = pendingBlocks.length;
    for (int i = 0; i < nrBlocks; i++){
      try {
        // filter out containingNodes that are marked for decommission.
        NumberReplicas number = countNodes(pendingBlocks[i]);
        if (number.liveReplicas() < numExpectedReplicas) {
          neededReplications.add(pendingBlocks[i], 
                                 number.liveReplicas(), 
                                 number.decommissionedReplicas,
                                 numExpectedReplicas);
        }
      } catch (IOException ioe) {
        LOG.error("checkReplicationFactor: block=" + pendingBlocks[i], ioe);
      }
    }
  }
  
  NumberReplicas countNodes(Block b) throws IOException {
    if(!lockManager.lockBlock(b.getBlockId())) {
      throw new IOException ("cannot get block lock for block=" + b);
    }
    try {
      return countNodesInternal(b);
    } finally {
      lockManager.unlockBlock(b.getBlockId());
    }
  }
  
  
  /**
   * Return the number of nodes that are live and decommissioned.
   */
  NumberReplicas countNodesInternal(Block b) throws IOException {
    Collection<Integer> nodes = null;
    nodes = stateManager.getDatanodeList(b.getBlockId());
    if (nodes == null) {
      return new NumberReplicas();
    }
    int count = 0;
    int live = 0;
    int corrupt = 0;
    int excess = 0;

    boolean corruptExist=false;
    for (Integer i : nodes) {
      DatanodeDescriptor node = getDataNodeDescriptorByID(i);
      if (node == null) {
        continue;
      }
      try{
        corruptExist=corruptReplicas.isReplicaCorrupt(b, node);
      }catch(IOException e){
        e.printStackTrace();
        continue;
      }
      if (corruptExist) {
        corrupt++;
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        count++;
      } else {
        if (excessReplicateMap.exist(node.getStorageID(), b)) {
          excess++;
        } else {
          live++;
        }
      }
    }
    return new NumberReplicas(live, count, corrupt, excess);
  }
  
  public DatanodeInfo[] datanodeReport( DatanodeReportType type)
      throws AccessControlException {
    ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(type);
    DatanodeInfo[] arr = new DatanodeInfo[results.size()];
    for (int i=0; i<arr.length; i++) {
      arr[i] = new DatanodeInfo(results.get(i));
    }
    return arr;
    
  }
  
  long[] getStats() {
    synchronized (datanodeMap) {
      return new long[] { this.capacityTotal, this.capacityUsed,
          this.capacityRemaining, this.underReplicatedBlocksCount,
          this.corruptReplicaBlocksCount, getMissingBlocksCount() };
    }
  }
  
  public long getMissingBlocksCount() {
    // not locking
    return Math.max(missingBlocksInPrevIter, missingBlocksInCurIter);
  }
  
  long getPreferredBlockSize(String filename) throws IOException {
    // this is a 'static' viariable in file table, so we don't need
    // to get the file lock
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(filename);
    if (nodeInfo == null) {
      throw new IOException("Unknown file: " + filename);
    }
    FileStatus status = nodeInfo.fs;
    if (status.isDir()) {
      throw new IOException("Getting block size of a directory: " + filename);
    }
    return status.getBlockSize();
  }
  
  /** Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @throws IOException if permission to access file is denied by the system 
   * @return object containing information regarding the file
   *         or null if file not found
   */
  FileStatus getFileInfo(String src) throws IOException {
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    if(nodeInfo != null) {
      return nodeInfo.fs;
    } else {
      return null;
    }
  }
  
  /** {@inheritDoc} */
  public void commitBlockSynchronization(Block lastblock,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
  ) throws IOException {
    LOG.info("commitBlockSynchronization(lastblock=" + lastblock
        + ", newgenerationstamp=" + newgenerationstamp + ", newlength="
        + newlength + ", newtargets=" + Arrays.asList(newtargets)
        + ", closeFile=" + closeFile + ", deleteBlock=" + deleteblock + ")");
    final BlockEntry oldblockinfo = stateManager.getStoredBlockBy(lastblock.getBlockId()); 
    if (oldblockinfo == null) {
      throw new IOException("Block (=" + lastblock + ") not found");
    }
    
    int fileId = oldblockinfo.getFileId();
    long blockId = oldblockinfo.getBlockId();
    
    lockManager.lockFileBlock(fileId, blockId);
    try {
      DbNodeFile file = fileManager.getFile(oldblockinfo.getFileId());
      if (file == null || !file.isUnderConstruction()) {
        throw new IOException("Unexpected block (=" + lastblock
            + ") since the file (=" + oldblockinfo.getFileId()
            + ") is not under construction");
      }
      DbNodePendingFile pendingFile = (DbNodePendingFile)file;
      unprotectedCommitBlockSynchronization(pendingFile, 
          lastblock, oldblockinfo, newgenerationstamp, newlength, 
          closeFile, deleteblock, newtargets);
      LOG.info("commitBlockSynchronization(newblock=" + lastblock + ", file="
          + pendingFile.getFilePath() + ", newgenerationstamp=" + newgenerationstamp + ", newlength="
          + newlength + ", newtargets=" + Arrays.asList(newtargets)
          + ") successful");
    } finally {
      lockManager.unlockFileBlock(fileId, blockId);
    }
  }
  
  void unprotectedCommitBlockSynchronization(DbNodePendingFile pendingFile, 
      Block lastblock, BlockEntry oldblockinfo,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
  ) throws IOException {

    // Remove old block from blocks map. This always have to be done
    // because the generation stamp of this block is changing.
    try {
      stateManager.removeBlock(lastblock.getBlockId());
    } catch (IOException ioe) {
      LOG.error("IOException occurs when calling blockmap's removeBlock: " +
          "blockId = " + lastblock.getBlockId(), ioe);     
    }

    if (deleteblock) {
      pendingFile.removeBlock(lastblock);
      pendingFile.refreshBlocks(); // blocks have changed
    } else {
      // update last block, construct newblockinfo and add it to the blocks map
      lastblock.set(lastblock.getBlockId(), newlength, newgenerationstamp);
      // use last block's fileIndex from blocksmap instead of
      // calculating length() -1 to get the file index
      int lastBlockIndex = oldblockinfo.getIndex();
      int fileid = pendingFile.getFileId();
      try {
        stateManager.addBlockToFileBy(lastblock, fileid, lastBlockIndex);
      } catch (IOException ioe) {
        LOG.error(
            "IOException occurs when calling blockmap's addBlockToFileBy: "
                + "block = " + lastblock + ", fileid = " + fileid
                + ", index = " + lastBlockIndex, ioe);
        throw ioe;
      }

      BlockInfo newblockinfo = new BlockInfo(lastblock, pendingFile
          .getReplication());
      // find the DatanodeDescriptor objects
      // There should be no locations in the blocksMap till now because the
      // file is underConstruction
      DatanodeDescriptor[] descriptors = null;
      List<DatanodeDescriptor> descriptorsList = new ArrayList<DatanodeDescriptor>();
      for (int i = 0; i < newtargets.length; i++) {
        // We don't use getDatanode here since that method can
        // throw. If we were to throw an exception during this commit
        // process, we'd fall out of sync since DNs have already finalized
        // the block with the new GS.
        DatanodeDescriptor node = datanodeMap.get(newtargets[i].getStorageID());
        if (node != null) {
          if (closeFile) {
            // If we aren't closing the file, we shouldn't add it to the
            // block list for the node, since the block is still under
            // construction there. (in getAdditionalBlock, for example
            // we don't add to the block map for the targets)
            stateManager.receiveBlockFromDatanodeBy(node.getNodeid(), lastblock
                .getBlockId(), lastblock.getNumBytes(), lastblock
                .getGenerationStamp());
          }
          descriptorsList.add(node);
        } else {
          LOG.error("commitBlockSynchronization included a target DN "
              + newtargets[i] + " which is not known to NN. Ignoring.");
        }
      }
      if (!descriptorsList.isEmpty()) {
        descriptors = descriptorsList.toArray(new DatanodeDescriptor[0]);
      }
      // add locations into the INodeUnderConstruction
      pendingFile.refreshBlocks(); // blocks have changed
      pendingFile.setLastBlock(newblockinfo, descriptors);
    }

    // If this commit does not want to close the file, persist
    // blocks only if append is supported and return
    if (!closeFile) {
      if (supportAppends) {
      }
      LOG.info("commitBlockSynchronization(" + lastblock + ") successful");
      return;
    }

    // remove lease, close file
    finalizeDbNodePendingFileUnder(pendingFile);
  }
  
  /* updates a block in under replication queue */
  void updateNeededReplications(Block block, int curReplicasDelta,
      int expectedReplicasDelta, int fileReplication) throws IOException {
    NumberReplicas repl = countNodesInternal(block);
    int curExpectedReplicas = fileReplication;
    neededReplications.update(block, repl.liveReplicas(),
        repl.decommissionedReplicas(), curExpectedReplicas, curReplicasDelta,
        expectedReplicasDelta);
  }

  int getBlockReplicationByFileId(int fileId) throws IOException {
    FileStatus st = null;
    try {
      st = fileManager.getFileStatus(fileId);
    } catch (IOException ignored) {}
    if(st != null){
      return st.getReplication();
    } else {
      throw new IOException("Can't find the file of " 
          + fileId + " from namespace! ");
    }
  }
  
  /**
   * Invalidates the given block on the given datanode.
   */
  private void invalidateBlock(Block blk, DatanodeDescriptor dnDesc)
      throws IOException {
    NameNode.stateChangeLog.info("DIR* NameSystem.invalidateBlock: " + blk
        + " on " + dnDesc.getName());
    if (dnDesc == null) {
      throw new IOException("Cannot invalidate block " + blk
          + " because datanode is null");
    }

    // Check how many copies we have of the block. If we have at least one
    // copy on a live node, then we can delete it.
    int count = countNodesInternal(blk).liveReplicas();
    if (count > 1) {
      addToInvalidates(blk, dnDesc);
      removeStoredBlock(blk, dnDesc);
      NameNode.stateChangeLog.info("BLOCK* NameSystem.invalidateBlocks: "
          + blk + " on " + dnDesc.getName() + " listed for deletion.");
    } else {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.invalidateBlocks: " + blk
          + " on " + dnDesc.getName() + " is the only copy and was not deleted.");
    }
  }
  
  /**
   * Modify (block-->datanode) map.  Possibly generate 
   * replication tasks, if the removed block is still valid.
   */
  void removeStoredBlock(Block block, DatanodeDescriptor node) throws IOException {
    NameNode.stateChangeLog.info("BLOCK* NameSystem.removeStoredBlock: "
        + block + " from " + node.getName());
    int nodeid = node.getNodeid(); 
    try {
      stateManager.removeBlockReplicationOnDatanodeBy(nodeid, block.getBlockId());
      NameNode.stateChangeLog.info("BLOCK* NameSystem.removeStoredBlock: "
            + block + " been removed from node " + node);
    } catch (IOException ioe) {
      LOG.error("IOException occurs when calling blockmap's removeBlockReplicationOnDatanodeBy: " +
          "nodeId = " + nodeid + ", blockId = " + block.getBlockId(), ioe);
      return;
    }
    //
    // It's possible that the block was removed because of a datanode
    // failure. If the block is still valid, check if replication is
    // necessary. In that case, put block on a possibly-will-
    // be-replicated list.
    //
    BlockEntry be = null;
    try {
      be = stateManager.getStoredBlockBy(block.getBlockId());
    } catch (IOException ioe) {
      LOG.error("IOException occurs when calling blockmap's getStoredBlockBy: " +
          "blockId = " + block.getBlockId(), ioe);
    }
    if (be != null) {
      // decrementSafeBlockCount(block);
      updateNeededReplications(block, -1, 
          0, getBlockReplicationByFileId(be.getFileId()));
    }

    //
    // We've removed a block from a node, so it's definitely no longer
    // in "excess" there.
    //
    try {
      if(excessReplicateMap.remove(node.getStorageID(), block)) {
        excessBlocksCount--;
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: "
            + block + " is removed from excessBlocks");
      }
    }catch(IOException e) {
      LOG.info("removeStoredBlock: IOException occurs for block=" + block + ", and datanodeId=" +
          node, e);
    }
    
    // Remove the replica from corruptReplicas
    corruptReplicas.removeFromCorruptReplicasMap(block, node);
  }
  
  /**
   * Adds block to list of blocks which will be invalidated on 
   * specified datanode and log the move
   * @param b block
   * @param n datanode
   */
  void addToInvalidates(Block b, DatanodeInfo n) {
    addToInvalidatesNoLog(b, n);
    NameNode.stateChangeLog.info("BLOCK* NameSystem.addToInvalidates: "
        + b.getBlockName() + " is added to invalidSet of " + n.getName());
  }
  
  /**
   * Adds block to list of blocks which will be invalidated on specified
   * datanode
   * @param b block
   * @param n datanode
   */
  private void addToInvalidatesNoLog(Block b, DatanodeInfo n) {
    try {
      recentInvalidateSets.put(n.getStorageID(), b);
      pendingDeletionBlocksCount++;
    } catch (IOException e) {
      LOG.error("cann't put " + n.getStorageID() 
          + " into recentInvalidateSets", e);
    }
  }
  
  /**
   * The client has detected an error on the specified located blocks 
   * and is reporting them to the server.  For now, the namenode will 
   * mark the block as corrupt.  In the future we might 
   * check the blocks are actually corrupt. 
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    for (int i = 0; i < blocks.length; i++) {
      Block blk = blocks[i].getBlock();
      try {
        if(lockManager.lockBlock(blk.getBlockId())) {
          try {
            DatanodeInfo[] nodes = blocks[i].getLocations();
            for (int j = 0; j < nodes.length; j++) {
              DatanodeInfo dn = nodes[j];
              markBlockAsCorrupt(blk, dn);
            }
          } finally {
            lockManager.unlockBlock(blk.getBlockId());
          }
        } else {
          LOG.error("cannot get block lock for =" + blk);
        }
      }catch (IOException ioe) {
        LOG.error("reportBadBlocks for block=" + blk, ioe);
      }
    }
  }
  /**
   * Mark the block belonging to datanode as corrupt
   * @param blk Block to be marked as corrupt
   * @param dn Datanode which holds the corrupt replica
   */
  public void markBlockAsCorrupt(Block blk, DatanodeInfo dn)
    throws IOException {
    DatanodeDescriptor node = getDatanode(dn);
    if (node == null) {
      throw new IOException("Cannot mark block" + blk.getBlockName()
          + " as corrupt because datanode " + dn.getName()
          + " does not exist. ");
    }
    BlockEntry be = stateManager.getStoredBlockBy(blk.getBlockId());
    if (be == null) {
      // Check if the replica is in the blockMap, if not
      // ignore the request for now. This could happen when BlockScanner
      // thread of Datanode reports bad block before Block reports are sent
      // by the Datanode on startup
      NameNode.stateChangeLog.info("BLOCK NameSystem.markBlockAsCorrupt: "
          + "block " + blk + " could not be marked "
          + "as corrupt as it does not exists in " + "blocksMap");
    } else {
      // fixed the numbytes and genstamp according to recorded values
      // this is VERY important for corrupt block with wrong numbytes or timestamp
      blk.setNumBytes(be.getNumbytes());
      blk.setGenerationStamp(be.getGenerationStamp());
      
      int fileid = be.getFileId();
      // Add this replica to corruptReplicas Map
      FileStatus fsts = null;
      try {
        fsts = fileManager.getFileStatus(fileid);
      } catch (IOException e) {
        fsts = null;
      }
      if (fsts == null) {
        NameNode.stateChangeLog.info("BLOCK Namenode.markBlockAsCorrupt: "
            + "block " + blk + " could not be marked "
            + "as corrupt as it does not belong to " + "any file");
        addToInvalidates(blk, node);
        return;
      }
      corruptReplicas.addToCorruptReplicasMap(blk, node);
      if (countNodesInternal(blk).liveReplicas() > fsts.getReplication()) {
        // the block is over-replicated so invalidate the replicas immediately
        invalidateBlock(blk, node);
      } else {
        // add the block to neededReplication
        updateNeededReplications(blk, -1, 0, fsts.getReplication());
      }
    }
  }
  
  enum CompleteFileStatus {
    OPERATION_FAILED,
    STILL_WAITING,
    COMPLETE_SUCCESS
  }
  
  static class NumberReplicas {
    private int liveReplicas;
    private int decommissionedReplicas;
    private int corruptReplicas;
    private int excessReplicas;

    NumberReplicas() {
      initialize(0, 0, 0, 0);
    }

    NumberReplicas(int live, int decommissioned, int corrupt, int excess) {
      initialize(live, decommissioned, corrupt, excess);
    }

    void initialize(int live, int decommissioned, int corrupt, int excess) {
      liveReplicas = live;
      decommissionedReplicas = decommissioned;
      corruptReplicas = corrupt;
      excessReplicas = excess;
    }

    int liveReplicas() {
      return liveReplicas;
    }
    int decommissionedReplicas() {
      return decommissionedReplicas;
    }
    int corruptReplicas() {
      return corruptReplicas;
    }
    int excessReplicas() {
      return excessReplicas;
    }
    boolean isAllZero() {
      return (liveReplicas == 0) && 
             (decommissionedReplicas == 0) &&
             (corruptReplicas == 0) &&
             (excessReplicas == 0);    
    }
  } 
  
  public CompleteFileStatus completeFile(String src, String holder) throws IOException {
    if (safeMode.get()) {
      throw new SafeModeException("Cannot complete file " + src);
    }
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    if(nodeInfo != null) {
      if(lockManager.lockFile(nodeInfo.fid)) {
        try {
          CompleteFileStatus status = completeFileInternal(nodeInfo, holder);
          return status;
        } finally {
          lockManager.unlockFile(nodeInfo.fid);
        }
      }
    } else {
      throw new FileNotFoundException("src=" + src);
    }
    return null;
  }
  
  private CompleteFileStatus completeFileInternal(DbNodeInfo nodeInfo, 
                                                String holder) throws IOException {
    NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " 
        + nodeInfo.fid + " for " + holder);

    DbNodePendingFile pendingFile = checkLease(nodeInfo, holder);
    Block[] blocks = pendingFile.getBlocks();
    String src = pendingFile.getFilePath();
    if(blocks == null) {
      NameNode.stateChangeLog.warn("DIR* NameSystem.completeFile: "
          + "failed to complete " + src
          + " because dir.getFileBlocks() is null,"
          + " pending from " + pendingFile.getClientMachine());
      return CompleteFileStatus.OPERATION_FAILED;
    }
    if(!checkFileProgress(pendingFile, true)) {
      return CompleteFileStatus.STILL_WAITING;
    }

    finalizeDbNodePendingFileUnder(pendingFile);
    NameNode.stateChangeLog.info("DIR* NameSystem.completeFile: file " 
        + src + " is closed by " + holder);
    return CompleteFileStatus.COMPLETE_SUCCESS;
  }
  
  /**
   * The client would like to let go of the given block
   */
  public boolean abandonBlock(Block b, String src, String holder
      ) throws IOException {
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    if(nodeInfo == null) {
      throw new FileNotFoundException("src=" + src);
    }
    if(lockManager.lockFile(nodeInfo.fid)) {
      try {
        DbNodePendingFile pendingFile = checkLease(nodeInfo, holder);
        if(pendingFile != null) {
          //
          // Remove the block from the pending creates list
          //
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
                                        +b+"of file "+src);
          pendingFile.removeBlock(b);
          stateManager.removeBlock(b.getBlockId());
          corruptReplicas.removeFromCorruptReplicasMap(b);
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: "
              + b
              + " is removed from pendingCreates");
          return true;
        } else {
          return false;
        }
      } finally {
        lockManager.unlockFile(nodeInfo.fid);
      }
    } else {
      throw new IOException("cannot get file lock for " + src);
    }
  }
  

  private void checkLease(String holder, DbNode file) throws IOException {
    
    if (file == null) {
      Lease lease = leaseManager.getLease(holder, false);
      throw new LeaseExpiredException(" File does not exist. " +
                                      (lease != null ? lease.getHolder() :
                                       "Holder " + holder + 
                                       " does not have any open files."));
    }
    if (!file.isUnderConstruction()) {
      Lease lease = leaseManager.getLease(holder, false);
      throw new LeaseExpiredException("No lease on " + file + 
                                      " File is not open for writing. " +
                                      (lease != null ? lease.getHolder() :
                                       "Holder " + holder + 
                                       " does not have any open files."));
    }
    DbNodePendingFile pendingFile = (DbNodePendingFile)file;
    if (holder != null && !pendingFile.getClientName().equals(holder)) {
      throw new LeaseExpiredException("Lease mismatch on " + file + " owned by "
          + pendingFile.getClientName() + " but is accessed by " + holder);
    }
  }
  
  // make sure that we still have the lease on this file.
  private DbNodePendingFile checkLease(DbNodeInfo nodeInfo, String holder) throws IOException {
    DbNodeFile file = null;
    try {
      file = fileManager.getFile(nodeInfo.fid, nodeInfo.fs);
    } catch (IOException ignored) { }
    checkLease(holder, file);
    return (DbNodePendingFile)file;
  }
  
  /**
   * Check that the indicated file's blocks are present and
   * replicated.  If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   * @throws IOException 
   */
  boolean checkFileProgress(DbNodeFile file, boolean checkall) throws IOException {
    boolean ret = true, retry = false;
    do {
      if (checkall) {
        // check all blocks of the file.
        for (BlockInfo blockInfo : file.getBlocks()) {
          if (blockInfo.getDatanode() == null
              || blockInfo.getDatanode().length == 0) {
            ret = false;
            break;
          }
        }
      } else {
        // check the penultimate block of this file
        Block block = file.getPenultimateBlock();
        if (block != null) {
          BlockInfo blockInfo = (BlockInfo) block;
          if (blockInfo.getDatanode() == null
              || blockInfo.getDatanode().length == 0) {
            ret = false;
          }
        }
      }
      if (!ret && !retry) {
        try {
          /* wait an experimental time for all blocks having 
           * at lease one record */
          TimeUnit.MILLISECONDS.sleep(10);
        } catch (InterruptedException ignored) {}
        /* after sleep we try to get blocks again, for adfs's RPC is 
         * expensive compared with HDFS */
        file.refreshBlocks();
        retry = true;
        ret = true;
      } else {
        if(retry&&ret) {
          LOG.info("COMPLETE* good, retry hit for file=" 
              + file.getFilePath());
        }
        break;
      }
    } while (true);

    return ret;
  }
  
  /**
   * Stub for old callers pre-HDFS-630
   */
  public LocatedBlock getAdditionalBlock(String src, 
                                         String clientName
                                         ) throws IOException {
    return getAdditionalBlock(src, clientName, null);
  }
  
  public LocatedBlock getAdditionalBlock(String src, String clientName, 
      List<Node> excludedNodes) throws IOException {
    
    long fileLength, blockSize;
    int replication;
    DatanodeDescriptor clientNode = null;
    Block newBlock = null;
    
    if(NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.getAdditionalBlock: file "
          + src + " for " + clientName);
    }
    
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    DbNodePendingFile pendingFile = null;
    DatanodeDescriptor targets[] = null;
    
    if(nodeInfo != null) {
      
      if(lockManager.lockFile(nodeInfo.fid)) {
        try {
          pendingFile = checkLease(nodeInfo, clientName);
          if(pendingFile == null){
            throw new IOException("can't get the file id of " + src);
          }
          //
          // If we fail this, bad things happen!
          //
          if (!checkFileProgress(pendingFile, false)) {
            throw new NotReplicatedYetException("Not replicated yet:"
                + pendingFile.getFilePath());
          }
          
          fileLength = pendingFile.computeContentSummary().getLength();
          blockSize = pendingFile.getPreferredBlockSize();
          clientNode = pendingFile.getClientNode();
          replication = (int) pendingFile.getReplication();
          
          /* We don't need to place replicator between two critical sections
           * as HDFS does, because we use the fine-grained file lock */
          
          // choose targets for the new block to be allocated.
          targets = replicator.chooseTarget(replication,
              clientNode, excludedNodes, blockSize);
          if (targets.length < this.minReplication) {
            throw new IOException("File " + src + " could only be replicated to "
                + targets.length + " nodes, instead of " + minReplication);
          }
          
          newBlock = allocateBlock(src, pendingFile);
          pendingFile.setTargets(targets);
          for (DatanodeDescriptor dn : targets) {
            dn.incBlocksScheduled();
          }
          
        } finally {
          lockManager.unlockFile(nodeInfo.fid);
        }
      } else {
        throw new IOException("can't get the file lock for " + src);
      }
    } else {
    	throw new FileNotFoundException("File: "+src+" does not exist");
    }
    
    // Create next block
    return new LocatedBlock(newBlock, targets, fileLength);
  }
  
  private Block allocateBlock(String src, DbNodePendingFile pendingFile) throws IOException {
    
    long blockid = randBlockId.nextLong();
    while(stateManager.isBlockIdExists(blockid)){
      blockid = randBlockId.nextLong();
    }
    Block b = new Block(blockid, 0, 0);
    b.setGenerationStamp(getGenerationStamp());
    
    BlockInfo bi = new BlockInfo(b, pendingFile.getReplication());
    pendingFile.addBlock(bi);
    
    stateManager.addBlockToFileBy(b, pendingFile.getFileId(), pendingFile.getBlocks().length - 1);
    
    NameNode.stateChangeLog.info("BLOCK* NameSystem.allocateBlock: " + src + ". " + b);
    return b;
  }
  
  /**
   * Set owner for an existing file.
   * @throws IOException
   */
  public void setOwner(String src, String username, String group
      ) throws IOException {
  }
  
  /////////////////////////////////////////////////////////
  //
  // These methods are called by HadoopFS clients
  //
  /////////////////////////////////////////////////////////
  /**
   * Set permissions for an existing file.
   * @throws IOException
   */
  public void setPermission(String src, FsPermission permission
      ) throws IOException {
  }
  
  /**
   * Set replication for an existing file.
   * 
   * The NameNode sets new replication and schedules either replication of 
   * under-replicated data blocks or removal of the eccessive block copies 
   * if the blocks are over-replicated.
   * 
   * @see ClientProtocol#setReplication(String, short)
   * @param src file name
   * @param replication new replication
   * @return true if successful; 
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(String src, short replication) 
                                throws IOException {
    boolean status = false;
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    if(nodeInfo != null) {
      if(lockManager.lockFile(nodeInfo.fid)) {
        try {
          status = setReplicationInternal(nodeInfo.fid, replication);
          if (status && auditLog.isInfoEnabled()) {
            logAuditEvent(UserGroupInformation.getCurrentUGI(),
                          Server.getRemoteIp(),
                          "setReplication", src, null, null);
          }
        } finally {
          lockManager.unlockFile(nodeInfo.fid);
        }
      }
    }
    return status;
  }
  
  private boolean setReplicationInternal(int fid,
      short replication) throws IOException {
    FileStatus filestatus = null;
    try {
      filestatus = fileManager.getFileStatus(fid);
    } catch (IOException e) {
      LOG.error("setReplicationInternal: " +
      		"IOException occurs for fid=" + fid, e);
      return false;
    }
    if (filestatus.isDir()) {
      return false;
    }
    DbNodeFile file = null;
    try {
      file = fileManager.getFile(fid, filestatus);
    } catch(IOException ignored) {}
    if(file == null) {
      return false;
    }
    
    BlockInfo[] blks = file.getBlocks();
    if (blks == null || blks.length == 0) {
      return false;
    }
    long[] blkIds = new long[blks.length];
    for(int i = 0; i < blks.length; i++) {
      blkIds[i] = blks[i].getBlockId();
    }
    
    short oldreplication = filestatus.getReplication();
    if (replication == oldreplication) {
      return true;
    }
    
    // acquired blocks lock
    lockManager.lockBlocks(blkIds);
    try {
      stateManager.setReplication(file.getFilePath(), (byte)replication);
      
      // update needReplication priority queues
      for(int idx = 0; idx < blks.length; idx++) {
        updateNeededReplications(blks[idx], 0, replication-oldreplication, replication);
      }
      if (oldreplication > replication) {  
        // old replication > the new one; need to remove copies
        LOG.info("Reducing replication for file " + file.getFileId() 
                 + ". New replication is " + replication);
        for(int idx = 0; idx < blks.length; idx++) {
          processOverReplicatedBlock(blks[idx], replication, null, null);
        }
      } else { // replication factor is increased
        LOG.info("Increasing replication for file " + file.getFileId() 
            + ". New replication is " + replication);
      }
    } finally {
      lockManager.unlockBlocks(blkIds);
    }
    return true;
  }
  
  /**
   * Append to an existing file in the namespace.
   */
  LocatedBlock appendFile(String src, String holder, String clientMachine
      ) throws IOException {
    if (safeMode.get()) {
      throw new SafeModeException("Cannot create file" + src);
    }
    if (supportAppends == false) {
      throw new IOException("Append to hdfs not supported." +
                            " Please refer to dfs.support.append configuration parameter.");
    }
    DbNodeInfo nodeInfo = null;
    TimeTrackerAPI at = 
      TimeTrackerFactory.creatTimeTracker("appendFile("+src+")", 
          enableTimeTracker);
    ZKClient.SimpleLock lock = fileManager.getFileLock(src);
    if(lock != null && lock.trylock()) {
      try {
        at.set("zktrylock");
        nodeInfo = startFileInternal(src, null, holder, clientMachine, false, true, 
            (short)maxReplication, (long)0, at);
      } finally {
        lock.unlock();
        at.set("zkunlock");
      }
    } else {
      throw new IOException("cannot append file: src=" + src);
    }
    
    if(nodeInfo == null) {
      throw new IOException("cannot append file: src=" + src + "for fid is null");
    } else {
      // Create a LocatedBlock object for the last block of the file
      // to be returned to the client. Return null if the file does not
      // have a partial block at the end.
      LocatedBlock lb = null;
      if(lockManager.lockFile(nodeInfo.fid)) {
        at.set("dblock");
        try {
          // Need to re-check existence here, since the file may have been deleted
          // in between the synchronized blocks 
          DbNodePendingFile file = checkLease(nodeInfo, holder);
          at.set("checkLease");
          BlockInfo[] blocks = file.getBlocks();
          at.set("getBlocks");
          if (blocks != null && blocks.length > 0) {
            BlockInfo storedBlock = blocks[blocks.length-1];
            Block last = new Block(storedBlock.getBlockId(), storedBlock.getNumBytes(), storedBlock.getGenerationStamp());
            if (file.getPreferredBlockSize() > storedBlock.getNumBytes()) {
              long fileLength = file.computeContentSummary().getLength();
              DatanodeDescriptor[] targets = storedBlock.getDatanode(); 
              
              // set the locations of the last block in the lease record
              file.setLastBlock(storedBlock, targets);
              
              lb = new LocatedBlock(last, targets, 
                  fileLength-storedBlock.getNumBytes());
              
              // Remove block from replication queue.
              updateNeededReplications(last, 0, 0, file.getReplication());
              
              // remove this block from the list of pending blocks to be deleted. 
              // This reduces the possibility of triggering HADOOP-1349.    
              try {
                int blockCount = recentInvalidateSets.remove(last.getBlockId());
                if (blockCount > 0)
                  pendingDeletionBlocksCount -= blockCount;
              } catch (IOException e) {
                LOG.error("recentInvalidateSets: lastblk=" + last, e);
              }
            }
          }
          at.set("handleBlocks");
        } finally {
          lockManager.unlockFile(nodeInfo.fid);
          at.set("dbunlock");
          if(enableTimeTracker) {
            System.out.println(at.close());
          }
        }
        if (lb != null) {
          if (NameNode.stateChangeLog.isDebugEnabled()) {
            NameNode.stateChangeLog.debug("DIR* NameSystem.appendFile: file "
                +src+" for "+holder+" at "+clientMachine
                +" block " + lb.getBlock()
                +" block size " + lb.getBlock().getNumBytes());
          }
        } // end if lb != null
      } // end if lock
      return lb;
    }
  }
  
  /**
   * Create a new file entry in the namespace.
   * 
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   * 
   * @throws IOException if file name is invalid
   *         {@link FSDirectory#isValidToCreate(String)}.
   */
  DbNodeInfo startFile(String src, PermissionStatus permissions,
                 String holder, String clientMachine,
                 boolean overwrite, short replication, long blockSize
                ) throws IOException {
    if (safeMode.get()) {
      throw new SafeModeException("Cannot create file" + src);
    }
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUGI(),
                    Server.getRemoteIp(),
                    "create", src, null, null);
    }
    TimeTrackerAPI at = 
      TimeTrackerFactory.creatTimeTracker("startFile(" +src + ")", 
          enableTimeTracker);
    ZKClient.SimpleLock lock = fileManager.getFileLock(src);
    if(lock != null && lock.trylock()) {
      at.set("zktrylock");
      try {
        return startFileInternal(src, permissions, holder, clientMachine, overwrite, false,
            replication, blockSize, at);
      } finally {
        lock.unlock();
        at.set("zkuntrylock");
        System.out.println(at.close());
      }
    } else {
      throw new IOException("cannot create file: src=" + src);
    }
    
  }
  
  private DbNodeInfo startFileInternal(String src,
      PermissionStatus permissions,
      String holder, 
      String clientMachine, 
      boolean overwrite,
      boolean append,
      short replication,
      long blockSize,
      TimeTrackerAPI at) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: src=" + src
          + ", holder=" + holder
          + ", clientMachine=" + clientMachine
          + ", replication=" + replication
          + ", overwrite=" + overwrite
          + ", append=" + append);
    }
    if (!DFSUtil.isValidName(src)) {
      throw new IOException("Invalid file name: " + src);
    }

    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    at.set("getDbNodeInfo");
    if(nodeInfo != null && nodeInfo.fs.isDir() ){
      throw new IOException("Cannot create file " + src + 
          "; already exists as a directory. Or this file is being written right now");
    }
    
    try {
      DbNodeFile file = null;
      
      try {
        if(nodeInfo != null) {
          file = fileManager.getFile(nodeInfo.fid, nodeInfo.fs);
        }
      } catch(IOException ignored) {}
      at.set("getFile");
      recoverLeaseInternal(file, holder, clientMachine, false);
      at.set("recoverLeaseInternal");
      try {
        verifyReplication(src, replication, clientMachine);
      } catch(IOException e) {
        throw new IOException("failed to create " + e.getMessage());
      }
    
      if (append) {
        if (nodeInfo == null) {
          throw new FileNotFoundException("failed to append to non-existent file "
              + src + " on client " + clientMachine);
        } else if (nodeInfo.fs != null && nodeInfo.fs.isDir()) {
          throw new IOException("failed to append to directory " + src 
                                +" on client " + clientMachine);
        }
      } else if (!fileManager.isValidToCreate(src, nodeInfo)) {
        if (overwrite) {
          LOG.info("startFileInternal: overwrite is set true, " +
          		"frist delete existed file=" + src);
          deleteInternal(src, true);
        } else {
          throw new IOException("failed to create file " + src 
                                +" on client " + clientMachine
                                +" either because the filename is invalid or the file exists");
        }
      }
      
      DatanodeDescriptor clientNode = host2DataNodeMap.getDatanodeByHost(clientMachine);
      int clientNodeId = (clientNode!=null?clientNode.getNodeid():-1);
      if (append) {
        // Replace current node with a INodeUnderConstruction.
        // Recreate in-memory lease record.
        DbNodePendingFile cons = fileManager.addFile(nodeInfo.fid, nodeInfo.fs,
            permissions, holder, clientMachine, clientNodeId);
        at.set("addFile");
        leaseManager.addLease(cons.getClientName(), nodeInfo.fid);
        at.set("addLease");
      } else {
        // Now we can add the name to the filesystem. This file has no
        // blocks associated with it.
        // just increment global generation stamp
        nextGenerationStamp();
        at.set("nextGenerationStamp");
        nodeInfo = fileManager.create(src, overwrite, (byte)replication, blockSize);
        at.set("state.create");
        DbNodePendingFile cons = fileManager.addFile(nodeInfo.fid, nodeInfo.fs,
            permissions, holder, clientMachine, clientNodeId);
        at.set("addFile");
        leaseManager.addLease(cons.getClientName(), nodeInfo.fid);
        at.set("addLease");
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: "
                                     +"add "+src+" to namespace for "+holder);
        }
      }
    } catch(IOException e){
      NameNode.stateChangeLog.warn("DIR* NameSystem.startFile: "
          + e.getMessage());
      throw e;
    }
    LOG.info("startFileInternal completed: file=" + src + ", fileid=" + nodeInfo.fid);
    return nodeInfo;
  }
  
  /**
   * Recover lease;
   * Immediately revoke the lease of the current lease holder and start lease
   * recovery so that the file can be forced to be closed.
   *
   * @param src the path of the file to start lease recovery
   * @param holder the lease holder's name
   * @param clientMachine the client machine's name
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(String src, String holder, 
      String clientMachine) throws IOException {
    if (safeMode.get()) {
      throw new SafeModeException(
          "Cannot recover the lease of " + src + " in safe mode");
    }
    if (!DFSUtil.isValidName(src)) {
      throw new IOException("Invalid file name: " + src);
    }
    
    DbNodeInfo nodeInfo = fileManager.getDbNodeInfo(src);
    if (nodeInfo == null) {
      throw new FileNotFoundException("File not found " + src);
    } else {
      if(lockManager.lockFile(nodeInfo.fid)) {
        try {
          DbNodeFile file = null;
          try {
            if(nodeInfo != null) {
              file = fileManager.getFile(nodeInfo.fid, nodeInfo.fs);
            }
          } catch(IOException ignored) {}
          if (file == null) {
            throw new FileNotFoundException("File not found " + src);
          }
          if (!file.isUnderConstruction()) {
            return true;
          }
          recoverLeaseInternal(file, holder, clientMachine, true);
          return true;
        } finally {
          lockManager.unlockFile(nodeInfo.fid);
        }
      } else {
        return false;
      }
    }    
  }
  
  private void recoverLeaseInternal(DbNodeFile file, String holder, 
      String clientMachine, boolean force) throws IOException {
    if (file != null && file.isUnderConstruction()) {
      DbNodePendingFile pendingFile = (DbNodePendingFile)file;
      //
      // If the file is under construction , then it must be in our
      // leases. Find the appropriate lease record.
      //
      Lease lease = leaseManager.getLease(holder, true);
      //
      // We found the lease for this file. And surprisingly the original
      // holder is trying to recreate this file. This should never occur.
      //
      if (!force && lease != null) {
        Lease leaseFile = leaseManager.getLeaseById(pendingFile.getFileId());
        if (leaseFile != null && leaseFile.equals(lease)) {
          throw new AlreadyBeingCreatedException(
                    "failed to create file " + pendingFile + " for " + holder +
                    " on client " + clientMachine +
                    " because current leaseholder is trying to recreate file.");
        }
      }
      //
      // Find the original holder.
      //
      lease = leaseManager.getLease(pendingFile.getClientName(), true);
      if (lease == null) {
        throw new AlreadyBeingCreatedException(
                    "failed to create file " + pendingFile + " for " + holder +
                    " on client " + clientMachine +
                    " because pendingCreates is non-null but no leases found.");
      }
      if (force) {
        // close now: no need to wait for soft lease expiration and
        // close only the file src
        LOG.info("recoverLease: recover lease " + lease + ", file=" + pendingFile +
                 " from client " + pendingFile.getClientName());
        internalReleaseLeaseOne(lease, pendingFile.getFileId());
      } else {
        //
        // If the original holder has not renewed in the last SOFTLIMIT
        // period, then start lease recovery.
        //
        if (lease.expiredSoftLimit()) {
          LOG.info("startFile: recover lease " + lease + ", file=" + pendingFile +
              " from client " + pendingFile.getClientName());
          internalReleaseLease(lease, pendingFile.getFileId());
        }
        throw new AlreadyBeingCreatedException(
            "failed to create file " + pendingFile + " for " + holder +
            " on client " + clientMachine +
            ", because this file is already being created by " +
            pendingFile.getClientName() +
            " on " + pendingFile.getClientMachine());
      }
    }
  }
  

  
  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration.
   */
  private void verifyReplication(String src, 
                                 short replication, 
                                 String clientName 
                                 ) throws IOException {
    String text = "file " + src 
      + ((clientName != null) ? " on client " + clientName : "")
      + ".\n"
      + "Requested replication " + replication;

    if (replication > maxReplication)
      throw new IOException(text + " exceeds maximum " + maxReplication);
      
    if (replication < minReplication)
      throw new IOException( 
                            text + " is less than the required minimum " + minReplication);
  }
  
  /**
   * This is invoked when a lease expires. On lease expiry, 
   * all the files that were written from that dfsclient should be
   * recovered.
   */
  void internalReleaseLease(Lease lease, int fileId) throws IOException {
    if (lease.hasFiles()) {
      for (String p: lease.getFiles()) {
        internalReleaseLeaseOne(lease, Integer.parseInt(p));
      }
    } else {
      internalReleaseLeaseOne(lease, fileId);
    }
  }
  
  /**
   * Move a file that is being written to be immutable.
   * @param strFileId The fileId
   * @param lease The lease for the client creating the file
   */
  void internalReleaseLeaseOne(Lease lease, int fileId) throws IOException {
    LOG.info("Recovering lease=" + lease + ", fileId=" + fileId);
    
    DbNodeFile file = null;
    try {
      file = fileManager.getFile(fileId);
    } catch (IOException ignored) {}
    if(file == null) {
      final String message = "DIR* NameSystem.internalReleaseCreate: "
        + "attempt to release a create lock on "
        + fileId + " file does not exist.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }
    if (!file.isUnderConstruction()) {
      final String message = "DIR* NameSystem.internalReleaseCreate: "
        + "attempt to release a create lock on "
        + fileId + " but file is already closed.";
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }
   
    DbNodePendingFile pendingFile = (DbNodePendingFile)file;
    
    // Initialize lease recovery for pendingFile. If there are no blocks 
    // associated with this file, then reap lease immediately. Otherwise 
    // renew the lease and trigger lease recovery.
    if (pendingFile.getTargets() == null ||
        pendingFile.getTargets().length == 0) {
      if (pendingFile.getBlocks().length == 0) {
        finalizeDbNodePendingFileUnder(pendingFile);
        NameNode.stateChangeLog.warn("BLOCK*"
          + " internalReleaseLease: No blocks found, lease removed for " + 
          pendingFile);
        return;
      }
      // setup the Inode.targets for the last block from the blocksMap
      //
      BlockInfo last = pendingFile.getLastBlock();
      pendingFile.setTargets(last.getDatanode());
    }
    // start lease recovery of the last block for this file.
    DatanodeDescriptor primary = pendingFile.assignPrimaryDatanode();
    // check if this primary needs block recovery 
    if(primary != null && !heartbeats.contains(primary)) {
      externalDatanodeCommandsHandler.recoverBlocks(primary, Integer.MAX_VALUE);
    }
    Lease reassignedLease = reassignLease(
      lease, HdfsConstants.NN_RECOVERY_LEASEHOLDER, pendingFile);
    leaseManager.renewLease(reassignedLease);
  }
  
  private Lease reassignLease(Lease lease, String newHolder,
      DbNodePendingFile pendingFile) throws IOException {
    if (newHolder == null)
      return lease;
    pendingFile.setClientName(newHolder);
    return leaseManager.reassignLease(lease, pendingFile.getFileId(), newHolder);
  }
  
  public boolean containsBlock(String storageID,long blockID) throws IOException{
    BlockEntry  blockEntry = stateManager.getStoredBlockBy(blockID);
    synchronized(datanodeMap) {
      DatanodeDescriptor dnDesc = datanodeMap.get(storageID);
      if(dnDesc == null){
        LOG.warn("Cannot find the corresponding " +
            "data node id using data node storageID:" +storageID);
        return false;
      }
      int nodeid = dnDesc.getNodeid();
      if(blockEntry != null && blockEntry.getDatanodeIds().contains(nodeid))
        return true;
      else
        return false;
    }
  }
  
  public DatanodeDescriptor getDataNodeDescriptorByID(int id) {
    if (id != -1) { // datanode id == -1 is absolutely invalid
      synchronized (datanodeMap) {
        for (DatanodeDescriptor desc : datanodeMap.values()) {
          if (desc.getNodeid() == id) {
            return desc;
          }
        }
      } // end of synchronization
    }
    return null;
  }

  
  /**
   * Get block locations within the specified range.
   * @see #getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String src,
      long offset, long length) throws IOException {

    LocatedBlocks blocks = getBlockLocations(src, offset, length, true);
    if (blocks != null) {
      //sort the blocks
      DatanodeDescriptor client = host2DataNodeMap.getDatanodeByHost(
          clientMachine);
      for (LocatedBlock b : blocks.getLocatedBlocks()) {
        clusterMap.pseudoSortByDistance(client, b.getLocations());
      }
    }
    return blocks;
  }
  
  public LocatedBlocks getBlockLocations(String src, long offset, long length
  ) throws IOException {
    return getBlockLocations(src, offset, length, false);
  }
  
  public LocatedBlocks getBlockLocations(String src, long offset, long length,
      boolean doAccessTime) throws IOException {
    if (offset < 0) {
      throw new IOException("Negative offset is not supported. File: " + src);
    }
    if (length < 0) {
      throw new IOException("Negative length is not supported. File: " + src);
    }
    DbNodeInfo nodeInfo = null;
    DbNodeFile file = null;
    TimeTrackerAPI at = 
      TimeTrackerFactory.creatTimeTracker("open("+src+")", enableTimeTracker);
    try {
      nodeInfo = fileManager.getDbNodeInfo(src);
      at.set("getDbNodeInfo");
      if (nodeInfo != null) {
        if (lockManager.lockFile(nodeInfo.fid)) {
          at.set("dblock");
          try {
            file = fileManager.getFile(nodeInfo.fid, nodeInfo.fs);
            at.set("getFile");
            if(file != null) {
              final LocatedBlocks ret = getBlockLocationsInternal(file, offset,
                  length, Integer.MAX_VALUE, doAccessTime, at);
              if (auditLog.isInfoEnabled()) {
                logAuditEvent(UserGroupInformation.getCurrentUGI(), Server.getRemoteIp(),
                    "open", src, null, null);
              }
              LOG.info("getBlockLocations completed for src=" + 
                  src + ", fileLen=" + ret.getFileLength() + 
                  ", underconstruction=" + ret.isUnderConstruction());
              return ret;
            }
          } finally {
            lockManager.unlockFile(nodeInfo.fid);
            at.set("unlockFile");
          }
        } else {
          throw new IOException ("cannot get file lock for " + src);
        }
      }
    } catch (IOException ioe) {
      LOG.error("getBlockLocations: src=" + src, ioe);
    } finally {
      if(enableTimeTracker) {
        System.out.println(at.close());
      }
    }
    
    return null;
  }
  
  /**
   * Get block locations within the specified range.
   */
  private LocatedBlocks getBlockLocationsInternal(DbNodeFile file, 
                                                       long offset, 
                                                       long length, 
                                                       int nrBlocksToReturn,
                                                       boolean doAccessTime,
                                                       TimeTrackerAPI at) 
                                                       throws IOException {
    LOG.info("getBlockLocationsInternal: start handling file=" + file.getFilePath() + 
        ", offset=" + offset);
    
    BlockInfo[] blocks = file.getBlocks();
    at.set("getBlocks");
    if (blocks == null) {
      return null;
    }
    if (blocks.length == 0) {
      return new LocatedBlocks(0, new ArrayList<LocatedBlock>(blocks.length),
          file.isUnderConstruction());
    }
    List<LocatedBlock> results = new ArrayList<LocatedBlock>(blocks.length);

    int curBlk = 0;
    long curPos = 0, blkSize = 0;
    int nrBlocks = (blocks[0].getNumBytes() == 0) ? 0 : blocks.length;
    for (curBlk = 0; curBlk < nrBlocks; curBlk++) {
      blkSize = blocks[curBlk].getNumBytes();
      assert blkSize > 0 : "Block of size 0";
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }

    if (nrBlocks > 0 && curBlk == nrBlocks) { // offset >= end of file
      return null;
    }

    long endOff = offset + length;
    at.set("setupvars");
    do {
      // get block locations
      int numNodes = blocks[curBlk].getDatanode().length;
      int numCorruptNodes = countNodes(blocks[curBlk]).corruptReplicas();
      int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blocks[curBlk]);
      if (numCorruptNodes != numCorruptReplicas) {
        LOG.warn("Inconsistent number of corrupt replicas for " + blocks[curBlk]
            + "blockMap has " + numCorruptNodes
            + " but corrupt replicas map has " + numCorruptReplicas);
      }
      boolean blockCorrupt = false;
      DatanodeDescriptor[] machineSet = null;
      
      if (file.isUnderConstruction() && curBlk == blocks.length - 1 
      /* numNodes == 0 means there is no datanode reported to namenode yet*/
          && numNodes == 0) {
        // get unfinished block locations
        DbNodePendingFile pendingFile = (DbNodePendingFile)file;
        machineSet = pendingFile.getTargets();
        blockCorrupt = false;
      } else {
        blockCorrupt = (numCorruptNodes == numNodes);
        int numMachineSet = blockCorrupt ? numNodes : 
                            (numNodes - numCorruptNodes);
        machineSet = new DatanodeDescriptor[numMachineSet];
        if (numMachineSet > 0) {
          int idx = 0;
          for(int i = 0; i < numNodes; i++) {
            DatanodeDescriptor dn = blocks[curBlk].getDatanode(i);
            boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blocks[curBlk], dn);
            if (blockCorrupt || (!blockCorrupt && !replicaCorrupt))
              machineSet[idx++] = dn;
          }
        }
      }
	    results.add(new LocatedBlock(blocks[curBlk], machineSet, curPos, blockCorrupt));
	    curPos += blocks[curBlk].getNumBytes();
	    curBlk++;
    } while (curPos < endOff 
        && curBlk < blocks.length
        && results.size() < Integer.MAX_VALUE);
    at.set("handleBlocks");
    return file.createLocatedBlocks(results);
  }
  
  private long nextGenerationStamp(long current) throws IOException {
    long gs = generationStamp.nextStamp(current);
    return gs;
  }
  
  private long nextGenerationStamp() throws IOException {
    long gs = generationStamp.nextStamp();
    return gs;
  }
  
  /**
   * Verifies that the block is associated with a file that has a lease.
   * Increments, logs and then returns the stamp
   *
   * @param block block
   * @param fromNN if it is for lease recovery initiated by NameNode
   * @return a new generation stamp
   */  
  public long nextGenerationStampForBlock(Block block, boolean fromNN) throws IOException {
    if (isInSafeMode()) {
      throw new SafeModeException("Cannot get nextGenStamp for " + block);
    }
    if(lockManager.lockBlock(block.getBlockId())) {
      try {
        BlockEntry be = stateManager.getStoredBlockBy(block.getBlockId(), 
            block.getGenerationStamp());
        if (be == null) {
          String msg = block + " is already commited, storedBlock == null.";
          LOG.info(msg);
          throw new IOException(msg);
        }
        DbNodeFile file = null;
        try {
          file = fileManager.getFile(be.getFileId());
        } catch (IOException ignored) {}
        if(file == null || !file.isUnderConstruction()) {
          String msg = block + " is already commited, !fileINode.isUnderConstruction().";
          LOG.info(msg);
          throw new IOException(msg);
        }
        DbNodePendingFile pendingFile = (DbNodePendingFile)file;
        // Disallow client-initiated recovery once
        // NameNode initiated lease recovery starts
        if (!fromNN && 
            HdfsConstants.NN_RECOVERY_LEASEHOLDER.equals(pendingFile.getClientName())) {
          String msg = block +
            "is being recovered by NameNode, ignoring the request from a client";
          LOG.info(msg);
          throw new IOException(msg);
        }        
        if (!pendingFile.setLastRecoveryTime(now())) {
          String msg = block + " is already being recovered, ignoring this request.";
          LOG.info(msg);
          throw new IOException(msg);
        }
        return nextGenerationStamp(block.getGenerationStamp());
      } finally {
        lockManager.unlockBlock(block.getBlockId());
      }
    } else {
      throw new IOException("cannot get block lock for block=" + block);
    } 
  }
  
  /**
   * Current system time.
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
  }
  
  boolean setSafeMode(SafeModeAction action) throws IOException {
    if (action != SafeModeAction.SAFEMODE_GET) {
      // TODO: may add implementation further, but it's OK NOW. 
    }
    return safeMode.get();
  }
  
  public synchronized void cleanNamespaceAndBlocksmap() throws IOException{
    stateManager.format();
  }
  
  /**
   * Intervally update the datanodeMap in the memory, sync
   * it from bm service.
   */
  void updateDatanodesMap(boolean countInterval) {
    Collection<DatanodeInfo> allAliveDatanodes = null;
    try {
      if (countInterval) {
        allAliveDatanodes = stateManager
            .getAliveDatanodes(datanodeExpireInterval);
      } else {
        allAliveDatanodes = stateManager.getAliveDatanodes(Long.MAX_VALUE);
      }
    } catch (IOException ioe) {
      LOG.error(
          "updateDatanodesMap: calling blockmap's getAliveDatanodes failed. "
              + " countInterval=" + countInterval, ioe);
      return;
    }

    synchronized (datanodeMap) {
      if (allAliveDatanodes != null && allAliveDatanodes.size() > 0) {
        long capacityTotal = 0;
        long capacityUsed = 0;
        long capacityRemaining = 0;
        int totalLoad = 0;
        boolean isRefleshed = false;
        boolean isRemoved = false;
        for (DatanodeInfo dnInfo : allAliveDatanodes) {
          capacityTotal += dnInfo.getCapacity();
          capacityUsed += dnInfo.getDfsUsed();
          capacityRemaining += dnInfo.getRemaining();
          totalLoad += dnInfo.getXceiverCount();
          DatanodeDescriptor dnDescriptor = datanodeMap.get(dnInfo
              .getStorageID());
          if (dnDescriptor == null) {
            DatanodeDescriptor nodeDescr = new DatanodeDescriptor(dnInfo,
                NetworkTopology.DEFAULT_RACK, dnInfo.getHostName());
            dnDescriptor = nodeDescr;
            nodeDescr.isAlive = true;
            LOG.info("add datanode " + dnInfo
                + " into datanodeMap and mark it as alive");
            datanodeMap.put(dnInfo.getStorageID(), nodeDescr);
            datanodeMap.get(dnInfo.getStorageID()).updateFromBM(dnInfo);
          } else {
            LOG.info("update datanode " + dnInfo + " into datanodeMap");
            dnDescriptor.isAlive = true;
            isRefleshed = (dnDescriptor.getLastUpdate() < dnInfo
                .getLastUpdate());
            isRemoved = false;
            if (isRefleshed) {
              // make sure it is not called by registerDatanode
              StackTraceElement[] ste = Thread.currentThread().getStackTrace();
              if (ste != null && ste[2] != null
                  && !"registerDatanode".equals(ste[2].getMethodName())) {
                synchronized (heartbeats) {
                  /*
                   * if heartbeats contains this datanode, but the lastupdate
                   * value from block map is newer than local's. This means this
                   * datanode is alive but no longer reporting to this namenode.
                   */
                  if (heartbeats.contains(dnDescriptor)) {
                    heartbeats.remove(dnDescriptor);
                    LOG.warn(dnDescriptor.getName()
                        + " is no longer reporting to me!");
                    isRemoved = true;
                  }
                }
                /*
                 * move the rest commands to the 'external' channel, the number
                 * shouldn't be larger than Integer.MAX_VALUE
                 */
                if (isRemoved) {
                  externalDatanodeCommandsHandler.transferBlock(dnDescriptor,
                      Integer.MAX_VALUE);
                  externalDatanodeCommandsHandler.invalidateBlocks(
                      dnDescriptor, Integer.MAX_VALUE);
                }
              }
            }
            datanodeMap.get(dnInfo.getStorageID()).updateFromBM(dnInfo);
          }
          resolveNetworkLocation(dnDescriptor);
          synchronized (clusterMap) {
            if (!clusterMap.contains(dnDescriptor)) {
              LOG.info("add datanode " + dnInfo + " into clusterMap");
              clusterMap.add(dnDescriptor);
            }
          }
          if (dnDescriptor != null) {
            host2DataNodeMap.add(dnDescriptor);
          }
        }

        this.capacityTotal = capacityTotal;
        this.capacityUsed = capacityUsed;
        this.capacityRemaining = capacityRemaining;
        this.totalLoad = totalLoad;
      }

      if (datanodeMap.size() < this.minReplication) {
        safeMode.set(true);
      } else {
        safeMode.set(false);
      }
    } // end of sync datanodemap
  }

  private ObjectName mbeanName;
  /**
   * Register the FSNamesystem MBean using the name
   *        "hadoop:service=NameNode,name=FSNamesystemState"
   */
  void registerMBean(Configuration conf) {
    // We wrap to bypass standard mbean naming convention.
    // This wraping can be removed in java 6 as it is more flexible in 
    // package naming for mbeans and their impl.
    StandardMBean bean;
    try {
      myFSMetrics = new FSNamesystemMetrics(conf);
      bean = new StandardMBean(this,FSNamesystemMBean.class);
      mbeanName = MBeanUtil.registerMBean("NameNode", "FSNamesystemState", bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }

    LOG.info("Registered FSNamesystemStatusMBean");
  }
  
  /** Stop at and return the datanode at index (used for content browsing)*/
  @Deprecated
  private DatanodeDescriptor getDatanodeByIndex(int index) {
    int i = 0;
    for (DatanodeDescriptor node : datanodeMap.values()) {
      if (i == index) {
        return node;
      }
      i++;
    }
    return null;
  }
    
  @Deprecated
  public String randomDataNode() {
    int size = datanodeMap.size();
    int index = 0;
    if (size != 0) {
      index = r.nextInt(size);
      for(int i=0; i<size; i++) {
        DatanodeDescriptor d = getDatanodeByIndex(index);
        if (d != null && !d.isDecommissioned() && !isDatanodeDead(d) &&
            !d.isDecommissionInProgress()) {
          return d.getHost() + ":" + d.getInfoPort();
        }
        index = (index + 1) % size;
      }
    }
    return null;
  }

  
  public DatanodeDescriptor getRandomDatanode() {
    return replicator.chooseTarget(1, null, null, 0)[0];
  }
  
  boolean isInSafeMode() {
    if (safeMode == null)
      return false;
    return safeMode.get();
  }
  
  String getSafeModeTip() {
    if (!isInSafeMode())
      return "";
    return "No datanodes report to this namenode yet, " +
    		"Please check if datanodes are configured properly.";
  }
  
  @Override
  public String getFSState() {
    return "Operational";
  }
  
  public long getDbNodesTotal() {
    // TODO: need to query blockmap for the real value
    return -1;
  }

  @Override
  public long getBlocksTotal() {
    // TODO: need to query blockmap for the real value
    return -1;
  }

  @Override
  public long getCapacityTotal() {
    return getStats()[0];
  }
  
  /**
   * Total used space by data nodes for non DFS purposes such
   * as storing temporary files on the local file system
   */
  public long getCapacityUsedNonDFS() {
    long nonDFSUsed = 0;
    synchronized(datanodeMap){
      nonDFSUsed = capacityTotal - capacityRemaining - capacityUsed;
    }
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }

  @Override
  public long getCapacityRemaining() {
    return getStats()[2];
  }
  
  /**
   * Total remaining space by data nodes as percentage of total capacity
   */
  public float getCapacityRemainingPercent() {
    synchronized(datanodeMap){
      if (capacityTotal <= 0) {
        return 0;
      }

      return ((float)capacityRemaining * 100.0f)/(float)capacityTotal;
    }
  }

  @Override
  public long getCapacityUsed() {
    return getStats()[1];
  }
  
  /**
   * Total used space by data nodes as percentage of total capacity
   */
  public float getCapacityUsedPercent() {
    synchronized(datanodeMap){
      if (capacityTotal <= 0) {
        return 100;
      }

      return ((float)capacityUsed * 100.0f)/(float)capacityTotal;
    }
  }

  @Override
  public int getTotalLoad() {
    return this.totalLoad;
  }

  @Override
  public int numLiveDataNodes() {
    int numLive = 0;
    synchronized (datanodeMap) {   
      for(Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); 
                                                               it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        if (!isDatanodeDead(dn) ) {
          numLive++;
        }
      }
    }
    return numLive;
  }

  @Override
  public int numDeadDataNodes() {
    throw new UnsupportedOperationException("numDeadDataNodes");
  }
  
  /**
   * Get the total number of objects in the system. 
   */
  long getMaxObjects() {
    return maxFsObjects;
  }

  @Override
  public long getFilesTotal() {
    // TODO : we need to query namespace for the total files
    return Long.MAX_VALUE;
  }

  public long getPendingReplicationBlocks() {
    return pendingReplicationBlocksCount;
  }

  public long getUnderReplicatedBlocks() {
    return underReplicatedBlocksCount;
  }

  /** Returns number of blocks with corrupt replicas */
  public long getCorruptReplicaBlocks() {
    return corruptReplicaBlocksCount;
  }

  public long getScheduledReplicationBlocks() {
    return scheduledReplicationBlocksCount;
  }

  public long getPendingDeletionBlocks() {
    return pendingDeletionBlocksCount;
  }

  public long getExcessBlocks() {
    return excessBlocksCount;
  }
  
  public int getBlockCapacity() {
    // TODO : need to get block capacity
    return Integer.MAX_VALUE;
  }
  
  public Date getStartTime() {
    return new Date(systemStart); 
  }
  
  short getMaxReplication()     { return (short)maxReplication; }
  short getMinReplication()     { return (short)minReplication; }
  short getDefaultReplication() { return (short)defaultReplication; }
  
  public void stallReplicationWork()   { stallReplicationWork = true;   }
  public void restartReplicationWork() { stallReplicationWork = false;  }
  
  /*============================ For testcase  ================================= */
  
  int getNumberOfDatanodes(DatanodeReportType type) {
    return getDatanodeListForReport(type).size(); 
  }
  
  private ArrayList<DatanodeDescriptor> getDatanodeListForReport(
      DatanodeReportType type) {

    boolean listLiveNodes = type == DatanodeReportType.ALL || type == DatanodeReportType.LIVE;
    boolean listDeadNodes = type == DatanodeReportType.ALL || type == DatanodeReportType.DEAD;

    HashMap<String, String> mustList = new HashMap<String, String>();

    if (listDeadNodes) {
      // first load all the nodes listed in include and exclude files.
      for (Iterator<String> it = hostsReader.getHosts().iterator(); it.hasNext();) {
        mustList.put(it.next(), "");
      }
      for (Iterator<String> it = hostsReader.getExcludedHosts().iterator(); it.hasNext();) {
        mustList.put(it.next(), "");
      }
    }

    ArrayList<DatanodeDescriptor> nodes = null;
 
    /* This implementation is different from HDFS, for adfs's datanodeMap contains
     * only live nodes. */
    Collection<DatanodeInfo> allDatanodes = null;
    HashMap<String, DatanodeInfo> allDatanodeMap = new HashMap<String, DatanodeInfo>();
    try {
      allDatanodes = stateManager.getAliveDatanodes(Long.MAX_VALUE);
    } catch (IOException ingored) {}
    if(allDatanodes != null) {
      for (DatanodeInfo di : allDatanodes) {
        allDatanodeMap.put(di.getStorageID(), di);
      }
    }
    
    synchronized (datanodeMap) {  
      nodes = new ArrayList<DatanodeDescriptor>(datanodeMap.size() + mustList.size());

      for (Iterator<DatanodeDescriptor> it = datanodeMap.values().iterator(); it.hasNext();) {
        DatanodeDescriptor dn = it.next();
        boolean isDead = !dn.isAlive;
        if ((isDead && listDeadNodes) || (!isDead && listLiveNodes)) {
          nodes.add(dn);
        }
        // Remove any form of the this datanode in include/exclude lists.
        mustList.remove(dn.getName());
        mustList.remove(dn.getHost());
        mustList.remove(dn.getHostName());
        // Remove from allDatanodes as well
        allDatanodeMap.remove(dn.getStorageID());
      }
    }
    if (listDeadNodes) {
      // Add nodes listed in include and exclude files.
      for (Iterator<String> it = mustList.keySet().iterator(); it.hasNext();) {
        DatanodeDescriptor dn = new DatanodeDescriptor(new DatanodeID(it.next()));
        dn.setLastUpdate(0);
        dn.isAlive = false;
        nodes.add(dn);
        // remove from allDatanodeMap as well
        allDatanodeMap.remove(dn.getStorageID());
      }
      // Add remained nodes from allDatanodeMap
      for (DatanodeInfo di : allDatanodeMap.values()) {
        DatanodeDescriptor dn = new DatanodeDescriptor(new DatanodeID(di));
        dn.isAlive = false;
        nodes.add(dn);
      }
    }
    
    return nodes;
  }
  
  public void DFSNodesStatus(ArrayList<DatanodeDescriptor> live, 
                                          ArrayList<DatanodeDescriptor> dead) {
    ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(DatanodeReportType.ALL);
    for (Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (!node.isAlive)
        dead.add(node);
      else
        live.add(node);
    }
  }

}
