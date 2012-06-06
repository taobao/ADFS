/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.EnumSet;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo.AdminStates;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenSecretManager;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.namenode.UnderReplicatedBlocks.BlockIterator;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMBean;
import org.apache.hadoop.hdfs.server.namenode.metrics.FSNamesystemMetrics;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations.BlockWithLocations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.VersionInfo;
import org.mortbay.util.ajax.JSON;

import com.taobao.adfs.util.HashedBytes;

import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.datanode.Datanode;
import com.taobao.adfs.distributed.DistributedException;
import com.taobao.adfs.file.File;
import com.taobao.adfs.lease.Lease;
import com.taobao.adfs.state.StateManager;
import com.taobao.adfs.util.IpAddress;

/***************************************************
 * FSNamesystem does the actual bookkeeping work for the
 * DataNode.
 * 
 * It tracks several important tables.
 * 
 * 1) valid fsname --> blocklist (kept on disk, logged)
 * 2) Set of all valid blocks (inverted #1)
 * 3) block --> machinelist (kept in memory, rebuilt dynamically from reports)
 * 4) machine --> blocklist (inverted #2)
 * 5) LRU cache of updated-heartbeat machines
 ***************************************************/
public class FSNamesystem implements FSConstants, FSNamesystemMBean, NameNodeMXBean {
  public static final Log LOG = LogFactory.getLog(FSNamesystem.class);
  public static final String AUDIT_FORMAT = "ugi=%s\t" + // ugi
      "ip=%s\t" + // remote IP
      "cmd=%s\t" + // command
      "src=%s\t" + // src path
      "dst=%s\t" + // dst path (optional)
      "perm=%s"; // permissions (optional)

  private static final ThreadLocal<Formatter> auditFormatter = new ThreadLocal<Formatter>() {
    protected Formatter initialValue() {
      return new Formatter(new StringBuilder(AUDIT_FORMAT.length() * 4));
    }
  };

  private static final void logAuditEvent(UserGroupInformation ugi, InetAddress addr, String cmd, String src,
      String dst, HdfsFileStatus stat) {
    final Formatter fmt = auditFormatter.get();
    ((StringBuilder) fmt.out()).setLength(0);
    auditLog.info(fmt.format(AUDIT_FORMAT, ugi, addr, cmd, src, dst,
        (stat == null) ? null : stat.getOwner() + ':' + stat.getGroup() + ':' + stat.getPermission()).toString());
  }

  public static final Log auditLog = LogFactory.getLog(FSNamesystem.class.getName() + ".audit");

  // Default initial capacity and load factor of map
  public static final int DEFAULT_INITIAL_MAP_CAPACITY = 16;
  public static final float DEFAULT_MAP_LOAD_FACTOR = 0.75f;

  // FSNamesystemMetrics counter variables
  private FSNamesystemMetrics myFSMetrics;
  boolean isAccessTokenEnabled;
  BlockTokenSecretManager accessTokenHandler;
  private long accessKeyUpdateInterval;
  private long accessTokenLifetime;

  private DelegationTokenSecretManager dtSecretManager;

  volatile long pendingReplicationBlocksCount = 0L;
  volatile long corruptReplicaBlocksCount = 0L;
  volatile long underReplicatedBlocksCount = 0L;
  volatile long scheduledReplicationBlocksCount = 0L;
  volatile long excessBlocksCount = 0L;
  volatile long pendingDeletionBlocksCount = 0L;

  /**
   * Store blocks-->datanodedescriptor(s) map of corrupt replicas
   */
  public CorruptReplicasMap corruptReplicas = new CorruptReplicasMap();

  /**
   * Keeps a Collection for every named machine containing
   * blocks that have recently been invalidated and are thought to live on the machine in question.
   * Mapping: StorageID -> ArrayList<Block>
   */
  private Map<String, Collection<Block>> recentInvalidateSets = new TreeMap<String, Collection<Block>>();

  /**
   * Keeps a TreeSet for every named node. Each treeset contains
   * a list of the blocks that are "extra" at that location. We'll
   * eventually remove these extras.
   * Mapping: StorageID -> TreeSet<Block>
   */
  Map<String, Collection<Block>> excessReplicateMap = new TreeMap<String, Collection<Block>>();

  /**
   * Store set of Blocks that need to be replicated 1 or more times.
   */
  UnderReplicatedBlocks neededReplications = new UnderReplicatedBlocks();
  /**
   * We also store pending replication-orders. Set of: Block
   */
  PendingReplicationBlocks pendingReplications;

  Random r = new Random();

  // file lock ////
  Map<String, Integer> filelocks = new ConcurrentHashMap<String, Integer>();

  private final ConcurrentHashMap<HashedBytes, CountDownLatch> lockedFiles =
      new ConcurrentHashMap<HashedBytes, CountDownLatch>();

  private final ConcurrentHashMap<Integer, HashedBytes> lockIds = new ConcurrentHashMap<Integer, HashedBytes>();

  private final AtomicInteger lockIdGenerator = new AtomicInteger(1);

  int rowLockWaitDuration;

  static private Random rand = new Random();

  static final int DEFAULT_ROWLOCK_WAIT_DURATION = 30000;
  // file lock ////

  private ReentrantReadWriteLock safeModeLock;

  //
  // Threaded object that checks to see if we have been
  // getting heartbeats from all clients.
  //
  Daemon hbthread = null; // HeartbeatMonitor thread
  public Daemon lmthread = null; // LeaseMonitor thread
  Daemon smmthread = null; // SafeModeMonitor thread
  public Daemon replthread = null; // Replication thread
  Daemon nnrmthread = null; // NamenodeResourceMonitor thread

  private volatile boolean hasResourcesAvailable = false;
  private volatile boolean fsRunning = true;
  long systemStart = 0;

  // The maximum number of replicates we should allow for a single block
  private int maxReplication;
  // How many outgoing replication streams a given node should have at one time
  private int maxReplicationStreams;
  // MIN_REPLICATION is how many copies we need in place or else we disallow the write
  private int minReplication;
  // Default replication
  private int defaultReplication;
  // Variable to stall new replication checks for testing purposes
  private volatile boolean stallReplicationWork = false;
  // heartbeatRecheckInterval is how often namenode checks for expired datanodes
  private long heartbeatRecheckInterval;
  // heartbeatExpireInterval is how long namenode waits for datanode to report
  // heartbeat
  private long heartbeatExpireInterval;
  // replicationRecheckInterval is how often namenode checks for new replication work
  private long replicationRecheckInterval;
  // default block size of a file
  private long defaultBlockSize = 0;
  // allow appending to hdfs files
  private boolean supportAppends = true;
  // resourceRecheckInterval is how often namenode checks for the disk space availability
  private long resourceRecheckInterval;
  // The actual resource checker instance.
  NameNodeResourceChecker nnResourceChecker;

  /**
   * Last block index used for replication work.
   */
  private int replIndex = 0;
  private long missingBlocksInCurIter = 0;
  private long missingBlocksInPrevIter = 0;

  public static FSNamesystem fsNamesystemObject;
  /** NameNode RPC address */
  private InetSocketAddress nameNodeAddress = null; // TODO: name-node has this field, it should be removed here
  private SafeModeInfo safeMode; // safe mode information

  // datanode networktoplogy
  private DNSToSwitchMapping dnsToSwitchMapping;

  // for block replicas placement
  ReplicationTargetChooser replicator;

  private HostsFileReader hostsReader;
  private Daemon dnthread = null;

  private long maxFsObjects = 0; // maximum number of fs objects

  // Ask Datanode only up to this many blocks to delete.
  private int blockInvalidateLimit = DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_DEFAULT;

  // precision of access times.
  private long accessTimePrecision = 0;
  private String nameNodeHostName;

  /**
   * FSNamesystem constructor.
   */
  FSNamesystem(NameNode nn, Configuration conf) throws IOException {
    try {
      this.conf = (conf == null) ? new Configuration(true) : conf;
      namenode = nn;
      initialize(nn, conf);
    } catch (IOException e) {
      LOG.error(getClass().getSimpleName() + " initialization failed.", e);
      try {
        stopMonitor();
        throw e;
      } catch (Throwable t) {
        LOG.error(e);
        LOG.error(t);
        throw e;
      }
    }
  }

  void activateSecretManager() throws IOException {
    if (dtSecretManager != null) {
      dtSecretManager.startThreads();
    }
  }

  /**
   * Initialize FSNamesystem.
   */
  private void initialize(NameNode nn, Configuration conf) throws IOException {
    this.systemStart = now();
    this.safeModeLock = new ReentrantReadWriteLock();
    setConfigurationParameters(conf);

    this.nameNodeAddress = nn.getNameNodeAddress();
    this.registerMBean(conf); // register the MBean for the FSNamesystemStutus

    this.hostsReader = new HostsFileReader(conf.get("dfs.hosts", ""), conf.get("dfs.hosts.exclude", ""));

    this.dnsToSwitchMapping =
        ReflectionUtils.newInstance(conf.getClass("topology.node.switch.mapping.impl", ScriptBasedMapping.class,
            DNSToSwitchMapping.class), conf);

    /*
     * If the dns to switch mapping supports cache, resolve network
     * locations of those hosts in the include list,
     * and store the mapping in the cache; so future calls to resolve
     * will be fast.
     */
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      dnsToSwitchMapping.resolve(new ArrayList<String>(hostsReader.getHosts()));
    }

    InetSocketAddress socAddr = NameNode.getAddress(conf);
    this.nameNodeHostName = socAddr.getHostName();

    stateManager =
        new StateManager(namenode.fileRepository, namenode.blockRepository, namenode.datanodeRepository,
            namenode.leaseRepository);
    corruptReplicas = new CorruptReplicasMap();
    excessReplicateMap = new TreeMap<String, Collection<Block>>();
    neededReplications = new UnderReplicatedBlocks();
    pendingReplications =
        new PendingReplicationBlocks(namenode.conf.getInt("dfs.replication.pending.timeout.sec", -1) * 1000L);
    recentInvalidateSets = new TreeMap<String, Collection<Block>>();
  }

  public static Collection<java.io.File> getNamespaceDirs(Configuration conf) {
    return null;
  }

  public static Collection<java.io.File> getNamespaceEditsDirs(Configuration conf) {
    return null;
  }

  /**
   * Initializes some of the members from configuration
   */
  private void setConfigurationParameters(Configuration conf) throws IOException {
    fsNamesystemObject = this;
    this.replicator = new ReplicationTargetChooser(conf.getBoolean("dfs.replication.considerLoad", true), this);
    this.defaultReplication = conf.getInt("dfs.replication", 3);
    this.maxReplication = conf.getInt("dfs.replication.max", 512);
    this.minReplication = conf.getInt("dfs.replication.min", 1);
    if (minReplication <= 0)
      throw new IOException("Unexpected configuration parameters: dfs.replication.min = " + minReplication
          + " must be greater than 0");
    if (maxReplication >= (int) Short.MAX_VALUE)
      throw new IOException("Unexpected configuration parameters: dfs.replication.max = " + maxReplication
          + " must be less than " + (Short.MAX_VALUE));
    if (maxReplication < minReplication)
      throw new IOException("Unexpected configuration parameters: dfs.replication.min = " + minReplication
          + " must be less than dfs.replication.max = " + maxReplication);
    this.maxReplicationStreams = conf.getInt("dfs.max-repl-streams", 2);
    long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
    this.heartbeatRecheckInterval = conf.getInt("heartbeat.recheck.interval", 5 * 60 * 1000); // 5 minutes
    this.heartbeatExpireInterval = 2 * heartbeatRecheckInterval + 10 * heartbeatInterval;
    this.replicationRecheckInterval = conf.getInt("dfs.replication.interval", 3) * 1000L;
    this.defaultBlockSize = conf.getLong("dfs.block.size", DEFAULT_BLOCK_SIZE);
    this.maxFsObjects = conf.getLong("dfs.max.objects", 0);

    // default limit
    this.blockInvalidateLimit = Math.max(this.blockInvalidateLimit, 20 * (int) (heartbeatInterval / 1000));
    // use conf value if it is set.
    this.blockInvalidateLimit = conf.getInt(DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY, this.blockInvalidateLimit);
    LOG.info(DFSConfigKeys.DFS_BLOCK_INVALIDATE_LIMIT_KEY + "=" + this.blockInvalidateLimit);

    this.accessTimePrecision = conf.getLong("dfs.access.time.precision", 0);
    this.supportAppends = conf.getBoolean("dfs.support.append", false);
    this.isAccessTokenEnabled = conf.getBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, false);
    if (isAccessTokenEnabled) {
      this.accessKeyUpdateInterval =
          conf.getLong(DFSConfigKeys.DFS_BLOCK_ACCESS_KEY_UPDATE_INTERVAL_KEY, 600) * 60 * 1000L; // 10 hrs
      this.accessTokenLifetime = conf.getLong(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_LIFETIME_KEY, 600) * 60 * 1000L; // 10
      // hrs
    }
    LOG.info("isAccessTokenEnabled=" + isAccessTokenEnabled + " accessKeyUpdateInterval=" + accessKeyUpdateInterval
        / (60 * 1000) + " min(s), accessTokenLifetime=" + accessTokenLifetime / (60 * 1000) + " min(s)");
  }

  /**
   * Return the default path permission when upgrading from releases with no
   * permissions (<=0.15) to releases with permissions (>=0.16)
   */
  protected PermissionStatus getUpgradePermission() {
    return null;
  }

  /**
   * Return the FSNamesystem object
   * 
   */
  public static FSNamesystem getFSNamesystem() {
    return fsNamesystemObject;
  }

  public static StateManager getFSStateManager() {
    return fsNamesystemObject.stateManager;
  }

  NamespaceInfo getNamespaceInfo() {
    return new NamespaceInfo(0, 0, 0);
  }

  /** Is this name system running? */
  boolean isRunning() {
    return fsRunning;
  }

  /**
   * Dump all metadata into specified file
   */
  synchronized void metaSave(String filename) throws IOException {
  }

  long getDefaultBlockSize() {
    return defaultBlockSize;
  }

  long getAccessTimePrecision() {
    return accessTimePrecision;
  }

  private boolean isAccessTimeSupported() {
    return accessTimePrecision > 0;
  }

  /* get replication factor of a block */
  private int getReplication(Block block) throws IOException {
    File file = stateManager.findFileByBlockId(block.getBlockId());
    // block does not belong to any file
    if (file == null) return 0;
    assert !file.isDir() : "Block cannot belong to a directory.";
    return file.replication;
  }

  /** updates a block in under replication queue */
  void updateNeededReplications(Block block, int curReplicasDelta, int expectedReplicasDelta) throws IOException {
    NumberReplicas repl = countNodes(block);
    int curExpectedReplicas = getReplication(block);
    neededReplications.update(block, repl.liveReplicas(), repl.decommissionedReplicas(), curExpectedReplicas,
        curReplicasDelta, expectedReplicasDelta);
  }

  /**
   * return a list of blocks & their locations on <code>datanode</code> whose
   * total size is <code>size</code>
   * 
   * @param datanode
   *          on which blocks are located
   * @param size
   *          total size of blocks
   */
  BlocksWithLocations getBlocks(DatanodeID nodeID, long size) throws IOException {
    DatanodeDescriptor node = nodeID == null ? null : stateManager.getDatanodeDescriptorByDatanodeId(nodeID.getId());
    if (node == null) {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.getBlocks: Asking for blocks from an unrecorded node " + nodeID);
      throw new IllegalArgumentException("Unexpected exception.  Got getBlocks message for datanode " + nodeID
          + ", but there is no info for it");
    }

    int numBlocks = node.numBlocks();
    if (numBlocks == 0) { return new BlocksWithLocations(new BlockWithLocations[0]); }
    List<com.taobao.adfs.block.Block> blockList = stateManager.findBlockByDatanodeId(node.getId(), false);
    Iterator<com.taobao.adfs.block.Block> iter = blockList.iterator();
    int startBlock = r.nextInt(numBlocks); // starting from a random block
    // skip blocks
    for (int i = 0; i < startBlock; i++) {
      iter.next();
    }
    List<BlockWithLocations> results = new ArrayList<BlockWithLocations>();
    long totalSize = 0;
    while (totalSize < size && iter.hasNext()) {
      totalSize += addBlock(iter.next(), results);
    }
    if (totalSize < size) {
      iter = blockList.iterator(); // start from the beginning
      for (int i = 0; i < startBlock && totalSize < size; i++) {
        totalSize += addBlock(iter.next(), results);
      }
    }

    return new BlocksWithLocations(results.toArray(new BlockWithLocations[results.size()]));
  }

  /**
   * Get access keys
   * 
   * @return current access keys
   */
  ExportedBlockKeys getBlockKeys() {
    return isAccessTokenEnabled ? accessTokenHandler.exportKeys() : ExportedBlockKeys.DUMMY_KEYS;
  }

  /**
   * Get all valid locations of the block & add the block to results
   * return the length of the added block; 0 if the block is not added
   * 
   * @throws IOException
   */
  private long addBlock(com.taobao.adfs.block.Block block, List<BlockWithLocations> results) throws IOException {
    BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(block.id);
    ArrayList<String> machineSet = new ArrayList<String>(blockEntry.getBlockList(true).size());
    for (com.taobao.adfs.block.Block tempBlock : blockEntry.getBlockList(true)) {
      DatanodeDescriptor datanodeDescriptor = stateManager.getDatanodeDescriptorByDatanodeId(tempBlock.datanodeId);
      String storageID = datanodeDescriptor == null ? null : datanodeDescriptor.getStorageID();
      if (storageID == null) continue;
      // filter invalidate replicas
      Collection<Block> blocks = recentInvalidateSets.get(storageID);
      if (blocks == null || !blocks.contains(block)) machineSet.add(storageID);
    }
    if (machineSet.size() == 0) {
      return 0;
    } else {
      results.add(new BlockWithLocations(new Block(block.id, block.generationStamp, block.length), machineSet
          .toArray(new String[machineSet.size()])));
      return blockEntry.getLength();
    }
  }

  // ///////////////////////////////////////////////////////
  //
  // These methods are called by HadoopFS clients
  //
  // ///////////////////////////////////////////////////////
  /**
   * Set permissions for an existing file.
   * 
   * @throws IOException
   */
  public void setPermission(String src, FsPermission permission) throws IOException {
    // TODO: adfs
  }

  /**
   * Set owner for an existing file.
   * 
   * @throws IOException
   */
  public void setOwner(String src, String username, String group) throws IOException {
    // TODO: adfs
  }

  /**
   * Get block locations within the specified range.
   * 
   * @see #getBlockLocations(String, long, long)
   */
  LocatedBlocks getBlockLocations(String clientMachine, String src, long offset, long length) throws IOException {
    LocatedBlocks blocks = getBlockLocations(src, offset, length, true, true);
    if (blocks != null) {
      // sort the blocks
      DatanodeDescriptor client = stateManager.getDatanodeDescriptorByName(clientMachine);
      for (LocatedBlock b : blocks.getLocatedBlocks()) {
        stateManager.clusterMap.pseudoSortByDistance(client, b.getLocations());
      }
    }
    return blocks;
  }

  /**
   * Get block locations within the specified range.
   * 
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  public LocatedBlocks getBlockLocations(String src, long offset, long length) throws IOException {
    return getBlockLocations(src, offset, length, false, true);
  }

  /**
   * Get block locations within the specified range.
   * 
   * @see ClientProtocol#getBlockLocations(String, long, long)
   */
  public LocatedBlocks getBlockLocations(String src, long offset, long length, boolean doAccessTime,
      boolean needBlockToken) throws IOException {
    if (offset < 0) { throw new IOException("Negative offset is not supported. File: " + src); }
    if (length < 0) { throw new IOException("Negative length is not supported. File: " + src); }
    final LocatedBlocks ret =
        getBlockLocationsInternal(src, offset, length, Integer.MAX_VALUE, doAccessTime, needBlockToken);
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(), "open", src, null, null);
    }
    return ret;
  }

  private LocatedBlocks getBlockLocationsInternal(String src, long offset, long length, int nrBlocksToReturn,
      boolean doAccessTime, boolean needBlockToken) throws IOException {
    File file = stateManager.findFileByPath(src);
    if (file == null || file.isDir()) { return null; }
    if (doAccessTime && isAccessTimeSupported()) {
      file.atime = now();
      stateManager.updateFileByFile(file, File.ATIME);
    }

    List<BlockEntry> blockEntryList = stateManager.getBlockEntryListByFileId(file.id);
    if (blockEntryList == null) return null;
    if (blockEntryList.isEmpty())
      return new LocatedBlocks(0, new ArrayList<LocatedBlock>(0), file.isUnderConstruction());
    List<LocatedBlock> results = new ArrayList<LocatedBlock>(blockEntryList.size());

    // seek the block by offset
    int curBlk = 0;
    long curPos = 0, blkSize = 0;
    int blockNumber = (blockEntryList.get(0).getLength() == 0) ? 0 : blockEntryList.size();
    for (curBlk = 0; curBlk < blockNumber; curBlk++) {
      blkSize = blockEntryList.get(curBlk).getLength();
      assert blkSize > 0 : "Block of size " + blkSize;
      if (curPos + blkSize > offset) {
        break;
      }
      curPos += blkSize;
    }
    // offset >= end of file
    if (blockNumber > 0 && curBlk == blockNumber) return null;

    long endOff = offset + length;

    do {
      // get block locations
      BlockEntry blockEntry = blockEntryList.get(curBlk);
      int numNodes = blockEntry.getBlockList(true).size();
      int numCorruptNodes = countNodes(blockEntry).corruptReplicas();
      int numCorruptReplicas = corruptReplicas.numCorruptReplicas(blockEntry.getHdfsBlock());
      if (numCorruptNodes != numCorruptReplicas) {
        LOG.warn("Inconsistent number of corrupt replicas for " + blockEntry.getHdfsBlock()
            + ", blockMap has corrupted " + numCorruptNodes + ", but corrupt replicas map has " + numCorruptReplicas);
      }
      DatanodeDescriptor[] machineSet = null;
      boolean blockCorrupt = false;

      List<com.taobao.adfs.block.Block> blockListExcludeUnderConstruction = blockEntry.getBlockList(true);
      if (file.isUnderConstruction() && curBlk == blockEntryList.size() - 1
          && blockListExcludeUnderConstruction.isEmpty()) {
        // get unfinished block locations
        machineSet = stateManager.getDatanodeDescriptorArrayByBlockList(blockEntry.getBlockList(false));
        blockCorrupt = false;
      } else {
        blockCorrupt = (numCorruptNodes == numNodes);
        int numMachineSet = blockCorrupt ? numNodes : (numNodes - numCorruptNodes);
        machineSet = new DatanodeDescriptor[numMachineSet];
        if (numMachineSet > 0) {
          numNodes = 0;
          for (com.taobao.adfs.block.Block block : blockListExcludeUnderConstruction) {
            DatanodeDescriptor dn = stateManager.getDatanodeDescriptorByDatanodeId(block.datanodeId);
            if (dn == null) continue;
            boolean replicaCorrupt = corruptReplicas.isReplicaCorrupt(blockEntry.getHdfsBlock(), dn);
            if (blockCorrupt || (!blockCorrupt && !replicaCorrupt)) machineSet[numNodes++] = dn;
          }
        }
      }
      LocatedBlock b = new LocatedBlock(blockEntry.getHdfsBlock(), machineSet, curPos, blockCorrupt);
      if (isAccessTokenEnabled && needBlockToken) {
        b.setBlockToken(accessTokenHandler.generateToken(b.getBlock(), EnumSet
            .of(BlockTokenSecretManager.AccessMode.READ)));
      }

      results.add(b);
      curPos += blockEntry.getLength();
      curBlk++;
    } while (curPos < endOff && curBlk < blockEntryList.size() && results.size() < nrBlocksToReturn);

    return new LocatedBlocks(BlockEntry.getTotalLength(blockEntryList), results, file.isUnderConstruction());
  }

  /**
   * stores the modification and access time for this inode.
   * The access time is precise upto an hour. The transaction, if needed, is
   * written to the edits log but is not flushed.
   */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    if (!isAccessTimeSupported() && atime != -1) { throw new IOException("Access time for hdfs is not configured. "
        + " Please set dfs.support.accessTime configuration parameter."); }
    if (isInSafeMode()) { throw new SafeModeException("Cannot set accesstimes  for " + src, safeMode); }
    File file = stateManager.findFileByPath(src);
    if (file == null) throw new FileNotFoundException("File " + src + " does not exist.");
    Integer lockid = getLock(src.getBytes(), true);
    try {
      file.mtime = mtime;
      file.atime = atime;
      stateManager.updateFileByFile(file, File.MTIME | File.ATIME);
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  /**
   * Set replication for an existing file.
   * 
   * The NameNode sets new replication and schedules either replication of
   * under-replicated data blocks or removal of the eccessive block copies
   * if the blocks are over-replicated.
   * 
   * @see ClientProtocol#setReplication(String, short)
   * @param src
   *          file name
   * @param replication
   *          new replication
   * @return true if successful;
   *         false if file does not exist or is a directory
   */
  public boolean setReplication(String src, short replication) throws IOException {
    boolean status = setReplicationInternal(src, replication);
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(), "setReplication", src, null, null);
    }
    return status;
  }

  private boolean setReplicationInternal(String src, short replication) throws IOException {
    if (isInSafeMode()) throw new SafeModeException("Cannot set replication for " + src, safeMode);
    verifyReplication(src, replication, null);

    File file = stateManager.findFileByPath(src);
    // file not found or is a directory
    if (file == null || file.isDir()) return false;
    // the same replication
    if (file.replication == replication) return true;
    int oldRepl = file.replication;
    file.replication = (byte) replication;
    Integer lockid = getLock(src.getBytes(), true);
    try {
      stateManager.updateFileByFile(file, File.REPLICATION);
      List<BlockEntry> blockEntryList = stateManager.getBlockEntryListOfFile(src);

      // update needReplication priority queues
      for (int idx = 0; idx < blockEntryList.size(); idx++)
        updateNeededReplications(blockEntryList.get(idx).getHdfsBlock(), 0, replication - oldRepl);

      if (oldRepl > replication) {
        // old replication > the new one; need to remove copies
        LOG.info("Reducing replication for file " + src + ". New replication is " + replication);
        for (int idx = 0; idx < blockEntryList.size(); idx++)
          processOverReplicatedBlock(blockEntryList.get(idx).getHdfsBlock(), replication, null, null);
      } else { // replication factor is increased
        LOG.info("Increasing replication for file " + src + ". New replication is " + replication);
      }
      return true;
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  long getPreferredBlockSize(String filename) throws IOException {
    File file = stateManager.findFileByPath(filename);
    if (file == null) { throw new IOException("Unknown file: " + filename); }
    if (file.isDir()) { throw new IOException("Getting block size of a directory: " + filename); }
    return file.blockSize;
  }

  /**
   * Check whether the replication parameter is within the range
   * determined by system configuration.
   */
  private void verifyReplication(String src, short replication, String clientName) throws IOException {
    if (replication > maxReplication || replication < minReplication) {
      String text =
          "file " + src + ((clientName != null) ? " on client " + clientName : "") + ".\n" + "Requested replication "
              + replication;
      if (replication > maxReplication) throw new IOException(text + " exceeds maximum " + maxReplication);
      if (replication < minReplication)
        throw new IOException(text + " is less than the required minimum " + minReplication);
    }
  }

  /**
   * Create a new file entry, need lock the file for adfs.
   * 
   * @see ClientProtocol#create(String, FsPermission, String, boolean, short, long)
   * 
   * @throws IOException
   *           if file name is invalid {@link FSDirectory#isValidToCreate(String)}.
   */
  void startFile(String src, PermissionStatus permissions, String holder, String clientMachine, boolean overwrite,
      boolean createParent, short replication, long blockSize) throws IOException {
    File file =
        startFileInternal(src, permissions, holder, clientMachine, overwrite, false, createParent, replication,
            blockSize);
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = StateManager.adfsFileToHdfsFileStatus(file);
      logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(), "create", src, null, stat);
    }
  }

  private File startFileInternal(String src, PermissionStatus permissions, String holder, String clientMachine,
      boolean overwrite, boolean append, boolean createParent, short replication, long blockSize) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: src=" + src + ", holder=" + holder + ", clientMachine="
          + clientMachine + ", createParent=" + createParent + ", replication=" + replication + ", overwrite="
          + overwrite + ", append=" + append);
    }
    if (isInSafeMode()) throw new SafeModeException("Cannot create file" + src, safeMode);
    if (!DFSUtil.isValidName(src)) throw new IOException("Invalid file name: " + src);

    File[] files = stateManager.findFilesByPath(src);
    File parentFile = files.length > 1 ? files[files.length - 2] : null;
    if (parentFile == null && !createParent) throw new FileNotFoundException(src + ": parent doesn't exist ");
    if (parentFile != null && !parentFile.isDir())
      throw new FileAlreadyExistsException(src + ": parent is not a directory");
    File file = files[files.length - 1];
    if (file != null && file.isIdentifierMatched()) return file;
    if (file != null && file.isDir()) throw new IOException(src + ": already exists as a directory.");
    if (!append && file != null && file.isIdentifierMatched() && !overwrite)
      throw new IOException(src + ": fail to create file on client " + clientMachine + " for the file exists");
    if (append && file == null)
      throw new FileNotFoundException(src + ": non-existent file for append on " + clientMachine);
    verifyReplication(src, replication, clientMachine);

    Integer lockid = getLock(src.getBytes(), true);
    try {
      try {
        recoverLeaseInternal(file, holder, clientMachine, false);
        stateManager.insertLeaseByHolder(holder);
        if (append) {
          file.leaseHolder = holder;
          file = stateManager.updateFileByFile(file, File.LEASEHOLDER);
        } else {
          while (true) {
            file = stateManager.insertFileByPath(src, (int) blockSize, 0, (byte) replication, false, holder);
            List<com.taobao.adfs.block.Block> blockList = stateManager.findBlockByFileId(file.id);
            if (blockList.isEmpty()) break;
            // recreate file with another id, blocks belonging to the old file will be deleted after block report
            stateManager.deleteFileByFile(file, false);
          }
        }
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* NameSystem.startFile: " + "add " + src + " for " + holder + " from "
              + clientMachine);
        }
        return file;
      } catch (IOException ie) {
        NameNode.stateChangeLog.warn("DIR* NameSystem.startFile: " + ie.getMessage());
        throw ie;
      }
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  /**
   * Recover lease;
   * Immediately revoke the lease of the current lease holder and start lease
   * recovery so that the file can be forced to be closed.
   * 
   * @param src
   *          the path of the file to start lease recovery
   * @param holder
   *          the lease holder's name
   * @param clientMachine
   *          the client machine's name
   * @return true if the file is already closed
   * @throws IOException
   */
  boolean recoverLease(String src, String holder, String clientMachine) throws IOException {
    if (isInSafeMode()) { throw new SafeModeException("Cannot recover the lease of " + src, safeMode); }
    if (!DFSUtil.isValidName(src)) { throw new IOException("Invalid file name: " + src); }
    File file = stateManager.findFileByPath(src);
    if (file == null) throw new FileNotFoundException("file not found " + src);
    if (!file.isUnderConstruction()) return true;
    if (file.isIdentifierMatched()) return false;
    Integer lockid = getLock(src.getBytes(), true);
    try {
      recoverLeaseInternal(file, holder, clientMachine, true);
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
    return false;
  }

  private void recoverLeaseInternal(File file, String holder, String clientMachine, boolean force) throws IOException {
    // If the file is under construction , then it must be in our leases. Find the appropriate lease record.
    if (file != null && file.isUnderConstruction()) {
      if (!force && holder.equals(file.leaseHolder)) { throw new AlreadyBeingCreatedException("failed to create "
          + file + " for " + holder + " on client " + clientMachine
          + ", because current leaseholder is trying to recreate file."); }
      Lease lease = stateManager.findLeaseByHolder(file.leaseHolder);
      if (lease == null) { throw new AlreadyBeingCreatedException("failed to create " + file + " for " + holder
          + " on client " + clientMachine + " because pendingCreates is non-null but no leases found."); }
      if (force) {
        // close now: no need to wait for soft lease expiration and close only the file src
        LOG.info("recoverLease: recover lease for " + file + " from client " + file.leaseHolder);
        internalReleaseLeaseOne(file, holder);
      } else {
        // If the original holder has not renewed in the last SOFTLIMIT period, then start lease recovery.
        if (StateManager.expiredSoftLimit(lease.time)) {
          LOG.info("startFile: recover lease for " + file + " from client " + file.leaseHolder);
          internalReleaseLeaseOne(file, holder);
          List<File> fileList = stateManager.findFileByLeaseHolder(file.leaseHolder);
          for (File fileWithSameLeaseHolder : fileList) {
            internalReleaseLeaseOne(fileWithSameLeaseHolder, holder);
          }
        }
        throw new AlreadyBeingCreatedException("failed to create file " + file + " for " + holder + " on client "
            + clientMachine + ", because this file is already being created by " + file.leaseHolder + " on "
            + file.clientIp);
      }
    }
  }

  /**
   * Append to an existing file in the namespace.
   */
  LocatedBlock appendFile(String src, String holder, String clientMachine) throws IOException {
    if (supportAppends == false) { throw new IOException("Append to hdfs not supported."
        + " Please refer to dfs.support.append configuration parameter."); }
    File file =
        startFileInternal(src, null, holder, clientMachine, false, true, false, (short) maxReplication, (long) 0);

    // Create a LocatedBlock object for the last block of the file to be returned to the client.
    // Return null if the file does not have a partial block at the end.
    LocatedBlock lb = null;
    Integer lockid = getLock(src.getBytes(), true);
    try {
      BlockEntry lastBlockEntry = stateManager.getLastBlockEntryByFileId(file.id);
      if (lastBlockEntry != null) {
        if (file.blockSize > lastBlockEntry.getLength()) {
          DatanodeDescriptor[] targets =
              stateManager.getDatanodeDescriptorArrayByBlockList(lastBlockEntry.getBlockList(false));
          lb = new LocatedBlock(lastBlockEntry.getHdfsBlock(), targets, file.length - lastBlockEntry.getLength());
          // Remove block from replication queue.
          updateNeededReplications(lastBlockEntry.getHdfsBlock(), 0, 0);
          // remove block from the list of pending blocks to be deleted. This reduces possibility of HADOOP-1349.
          for (Iterator<Collection<Block>> iter = recentInvalidateSets.values().iterator(); iter.hasNext();) {
            Collection<Block> v = iter.next();
            if (v.remove(lastBlockEntry.getHdfsBlock())) {
              pendingDeletionBlocksCount--;
              if (v.isEmpty()) {
                iter.remove();
              }
            }
          }
        }
      }
      if (lb != null) {
        if (NameNode.stateChangeLog.isDebugEnabled()) {
          NameNode.stateChangeLog.debug("DIR* NameSystem.appendFile: file " + src + " for " + holder + " at "
              + clientMachine + " block " + lb.getBlock() + " block size " + lb.getBlock().getNumBytes());
        }
      }
      return lb;
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  /**
   * Stub for old callers pre-HDFS-630
   */
  public LocatedBlock getAdditionalBlock(String src, String clientName) throws IOException {
    return getAdditionalBlock(src, clientName, null);
  }

  /**
   * The client would like to obtain an additional block for the indicated
   * filename (which is being written-to). Return an array that consists
   * of the block, plus a set of machines. The first on this list should
   * be where the client writes data. Subsequent items in the list must
   * be provided in the connection to the first datanode.
   * 
   * Make sure the previous blocks have been reported by datanodes and
   * are replicated. Will return an empty 2-elt array if we want the
   * client to "try again later".
   */
  public LocatedBlock getAdditionalBlock(String src, String clientName, List<Node> excludedNodes) throws IOException {
    NameNode.stateChangeLog.debug("BLOCK* NameSystem.getAdditionalBlock: file " + src + " for " + clientName);
    if (isInSafeMode()) throw new SafeModeException("Cannot add block to " + src, safeMode);
    checkFsObjectLimit();

    File file = stateManager.findFileByPath(src);
    checkLease(clientName, file);
    List<BlockEntry> blockEntryList = stateManager.getBlockEntryListByFileId(file.id);
    if (!checkFileProgress(blockEntryList, false)) throw new NotReplicatedYetException("Not replicated yet:" + src);

    // choose targets for the new block to be allocated.
    DatanodeDescriptor clientNode = stateManager.getDatanodeDescriptorByDatanodeIp(file.clientIp);
    DatanodeDescriptor targets[] = replicator.chooseTarget(file.replication, clientNode, excludedNodes, file.blockSize);
    if (targets.length < this.minReplication) { throw new IOException("File " + src + " could only be replicated to "
        + targets.length + " nodes, instead of " + minReplication); }

    // allocate new block
    com.taobao.adfs.block.Block newBlock = allocateBlock(file, blockEntryList.size(), targets);
    newBlock.length = 0;// Hadoop Block readFields will throw a IOException if length<0
    LocatedBlock b = new LocatedBlock(StateManager.adfsBlockToHdfsBlock(newBlock), targets, file.length);

    for (DatanodeDescriptor dn : targets) {
      dn.incBlocksScheduled();
    }
    return b;
  }

  /**
   * The client would like to let go of the given block
   */
  public boolean abandonBlock(Block b, String src, String holder) throws IOException {
    // Remove the block from the pending creates list
    NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: " + b + "of file " + src);
    if (isInSafeMode()) { throw new SafeModeException("Cannot abandon block " + b + " for fle" + src, safeMode); }
    Integer lockid = getLock(src.getBytes(), true);
    try {
      File file = stateManager.findFileByPath(src);
      checkLease(holder, file);
      stateManager.deleteBlockById(b.getBlockId());
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.abandonBlock: " + b + " is removed from pendingCreates");
      return true;
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  // make sure that we still have the lease on this file.
  private void checkLease(String holder, File file) throws IOException {
    if (file == null) throw new LeaseExpiredException("null file=" + file + ", holder=" + holder);
    if (file.isDir()) throw new LeaseExpiredException("dir file=" + file + ", holder=" + holder);
    if (!file.isUnderConstruction()) throw new LeaseExpiredException("completed file=" + file + ", holder=" + holder);
    if (holder == null) throw new LeaseExpiredException("file=" + file + ", null holder=" + holder);
    if (!holder.equals(file.leaseHolder)) throw new LeaseExpiredException("file=" + file + ", error holder=" + holder);
  }

  /**
   * The FSNamesystem will already know the blocks that make up the file.
   * Before we return, we make sure that all the file's blocks have
   * been reported by datanodes and are replicated correctly.
   */

  enum CompleteFileStatus {
    OPERATION_FAILED, STILL_WAITING, COMPLETE_SUCCESS
  }

  public CompleteFileStatus completeFile(String src, String holder) throws IOException {
    CompleteFileStatus status = completeFileInternal(src, holder);
    return status;
  }

  private CompleteFileStatus completeFileInternal(String src, String holder) throws IOException {
    NameNode.stateChangeLog.debug("DIR* NameSystem.completeFile: " + src + " for " + holder);
    if (isInSafeMode()) throw new SafeModeException("Cannot complete file " + src, safeMode);

    File file = stateManager.findFileByPath(src);
    checkLease(holder, file);

    List<BlockEntry> blockEntryList = stateManager.getBlockEntryListByFileId(file.id);
    if ((blockEntryList == null || blockEntryList.isEmpty()) && !file.leaseHolder.equals(holder)) {
      NameNode.stateChangeLog.warn("DIR* NameSystem.completeFile: " + "failed to complete " + src
          + " because stateManager.getBlocksByFileId(file.id) is null or empty," + " pending from " + file.leaseHolder);
      return CompleteFileStatus.OPERATION_FAILED;
    }
    if (!checkFileProgress(blockEntryList, true)) return CompleteFileStatus.STILL_WAITING;

    Integer lockid = getLock(src.getBytes(), true);
    try {
      finalizeINodeFileUnderConstruction(file, blockEntryList);
      NameNode.stateChangeLog.info("DIR* NameSystem.completeFile: file " + src + " is closed by " + holder);
      return CompleteFileStatus.COMPLETE_SUCCESS;
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  /**
   * Check all blocks of a file. If any blocks are lower than their intended
   * replication factor, then insert them into neededReplication
   * 
   * @throws IOException
   */
  private void checkReplicationFactor(File file, List<BlockEntry> blockEntryList) throws IOException {
    int numExpectedReplicas = file.replication;
    for (int i = 0; i < blockEntryList.size(); i++) {
      // filter out containingNodes that are marked for decommission.
      NumberReplicas number = countNodes(blockEntryList.get(i));
      if (number.liveReplicas() < numExpectedReplicas) {
        neededReplications.add(blockEntryList.get(i).getHdfsBlock(), number.liveReplicas(),
            number.decommissionedReplicas, numExpectedReplicas);
      }
    }
  }

  static Random randBlockId = new Random();

  /**
   * Allocate a block at the given pending filename
   */
  private com.taobao.adfs.block.Block allocateBlock(File file, int fileIndex, DatanodeDescriptor[] targets)
      throws IOException {
    com.taobao.adfs.block.Block block = new com.taobao.adfs.block.Block();
    block.length = -1;
    block.generationStamp = file.version;
    block.fileId = file.id;
    block.fileIndex = fileIndex;
    boolean isIdAllocated = false;
    for (DatanodeDescriptor datanodeDescriptor : targets) {
      if (datanodeDescriptor == null) continue;
      block.datanodeId = datanodeDescriptor.getId();
      if (!isIdAllocated) {
        while (true)
          try {
            block.id = FSNamesystem.randBlockId.nextLong();
            stateManager.insertBlockByBlock(block);
            isIdAllocated = true;
            break;
          } catch (DistributedException e) {
            throw e;
          } catch (Throwable t) {
            LOG.trace(t);
            continue;
          }
      } else stateManager.insertBlockByBlock(block);
    }
    NameNode.stateChangeLog.info("BLOCK* NameSystem.allocateBlock: " + block + " for " + file);
    return block;
  }

  /**
   * Check that the indicated file's blocks are present and
   * replicated. If not, return false. If checkall is true, then check
   * all blocks, otherwise check only penultimate block.
   * 
   * @throws IOException
   */
  boolean checkFileProgress(List<BlockEntry> blockEntryList, boolean checkall) throws IOException {
    if (blockEntryList == null) return true;
    if (checkall) {
      // check all blocks of the file.
      for (BlockEntry blockEntry : blockEntryList) {
        if (blockEntry.getBlockList(true).size() < this.minReplication) return false;
      }
    } else {
      // check the penultimate block of this file
      BlockEntry penultimateBlockEntry =
          blockEntryList.size() < 2 ? null : blockEntryList.get(blockEntryList.size() - 2);
      if (penultimateBlockEntry != null) {
        if (penultimateBlockEntry.getBlockList(true).size() < this.minReplication) return false;
      }
    }
    return true;
  }

  /**
   * Remove a datanode from the invalidatesSet
   * 
   * @param n
   *          datanode
   */
  void removeFromInvalidates(String storageID) {
    Collection<Block> blocks = recentInvalidateSets.remove(storageID);
    if (blocks != null) {
      pendingDeletionBlocksCount -= blocks.size();
    }
  }

  /**
   * Adds block to list of blocks which will be invalidated on
   * specified datanode and log the move
   * 
   * @param b
   *          block
   * @param n
   *          datanode
   */
  void addToInvalidates(Block block, DatanodeInfo node, boolean writeLog) {
    if (block == null || node == null) return;
    Collection<Block> invalidateSet = recentInvalidateSets.get(node.getStorageID());
    if (invalidateSet == null) recentInvalidateSets.put(node.getStorageID(), invalidateSet = new HashSet<Block>());
    if (invalidateSet.add(block)) pendingDeletionBlocksCount++;
    if (writeLog) NameNode.stateChangeLog.info("BLOCK* NameSystem.addToInvalidates:" + block + "=>" + node.getName());
  }

  /**
   * Mark the block belonging to datanode as corrupt
   * 
   * @param blk
   *          Block to be marked as corrupt
   * @param dn
   *          Datanode which holds the corrupt replica
   */
  public void markBlockAsCorrupt(Block blk, DatanodeInfo dn, Integer lockid) throws IOException {
    DatanodeDescriptor node = dn == null ? null : stateManager.getDatanodeDescriptorByDatanodeId(dn.getId());
    if (node == null) { throw new IOException("Cannot mark block" + blk.getBlockName()
        + " as corrupt because datanode " + dn.getName() + " does not exist. "); }

    com.taobao.adfs.block.Block block = stateManager.findBlockByIdAndDatanodeId(blk.getBlockId(), dn.getId());
    if (block == null) {
      // Check if the replica is in the blockMap, if not ignore the request for now.
      // This could happen when BlockScanner thread of Datanode reports bad block before Block reports are sent by the
      // Datanode on startup
      NameNode.stateChangeLog.info("BLOCK NameSystem.markBlockAsCorrupt: " + "block " + blk + " could not be marked "
          + "as corrupt as it does not exists in " + "blocksMap");
    } else {
      File file = stateManager.findFileById(block.fileId);
      if (file == null) {
        NameNode.stateChangeLog.info("BLOCK NameSystem.markBlockAsCorrupt: " + "block " + blk + " could not be marked "
            + "as corrupt as it does not belong to " + "any file");
        // TODO: adfs: check
        addToInvalidates(blk, dn, true);
        return;
      }
      boolean fileLockedByCurrentThread = true;
      if (lockid == null) {
        lockid = getLock(file.path.getBytes(), true);
        fileLockedByCurrentThread = false;
      }
      try {
        // Add this replica to corruptReplicas Map
        corruptReplicas.addToCorruptReplicasMap(blk, node);
        BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(blk.getBlockId());
        if (countNodes(blockEntry).liveReplicas() > file.replication) {
          // TODO: adfs : the block is over-replicated so invalidate the replicas immediately
          invalidateBlock(blk, node);
        } else {
          // add the block to neededReplication
          updateNeededReplications(blk, -1, 0);
        }
      } finally {
        if (lockid != null && !fileLockedByCurrentThread) {
          releaseFileLock(lockid);
        }
      }
    }
  }

  /**
   * avoid compile error for ut
   */
  public void markBlockAsCorrupt(Block blk, DatanodeInfo dn) throws IOException {
    markBlockAsCorrupt(blk, dn, null);
  }

  /**
   * Invalidates the given block on the given datanode.
   */
  private void invalidateBlock(Block blk, DatanodeInfo dn) throws IOException {
    NameNode.stateChangeLog.info("DIR* NameSystem.invalidateBlock: " + blk + " on " + dn.getName());
    DatanodeDescriptor node = dn == null ? null : stateManager.getDatanodeDescriptorByDatanodeId(dn.getId());
    if (node == null) { throw new IOException("Cannot invalidate block " + blk + " because datanode " + dn.getName()
        + " does not exist."); }
    blk.setDatanodeId(dn.getId());

    // Check how many copies we have of the block. If we have at least one
    // copy on a live node, then we can delete it.
    int count = countNodes(blk).liveReplicas();
    if (count > 1) {
      addToInvalidates(blk, node, true);
      removeStoredBlock(blk, node);
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.invalidateBlocks: " + blk + " on " + dn.getName()
          + " listed for deletion.");
    } else {
      NameNode.stateChangeLog.info("BLOCK* NameSystem.invalidateBlocks: " + blk + " on " + dn.getName()
          + " is the only copy and was not deleted.");
    }
  }

  // //////////////////////////////////////////////////////////////
  // Here's how to handle block-copy failure during client write:
  // -- As usual, the client's write should result in a streaming
  // backup write to a k-machine sequence.
  // -- If one of the backup machines fails, no worries. Fail silently.
  // -- Before client is allowed to close and finalize file, make sure
  // that the blocks are backed up. Namenode may have to issue specific backup
  // commands to make up for earlier datanode failures. Once all copies
  // are made, edit namespace and return to client.
  // //////////////////////////////////////////////////////////////

  /** Change the indicated filename. */
  public boolean renameTo(String src, String dst) throws IOException {
    File file = renameToInternal(src, dst);
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = StateManager.adfsFileToHdfsFileStatus(file);
      logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(), "rename", src, dst, stat);
    }
    return true;
  }

  private File renameToInternal(String src, String dst) throws IOException {
    NameNode.stateChangeLog.debug("DIR* NameSystem.renameTo: " + src + " to " + dst);
    if (isInSafeMode()) throw new SafeModeException("Cannot rename " + src, safeMode);
    if (!DFSUtil.isValidName(dst)) throw new IOException("Invalid name: " + dst);

    // if target file has been inserted and identifier is matched, it means client is retrying
    File targetFile = stateManager.findFileByPath(dst);
    if (targetFile != null && targetFile.isIdentifierMatched()) return targetFile;

    File file = stateManager.findFileByPath(src);
    if (file == null) throw new IOException("not existed path: " + src);
    if (src.equals(dst)) return file;

    Integer lockid = getLock(src.getBytes(), true);
    try {
      // create parent directory for dst
      java.io.File javaFile = new java.io.File(dst);
      String parentPath = javaFile.getParent();
      File parentFile = stateManager.insertFileByPath(parentPath, 0, -1, (byte) 0, false, null);

      // change parent directory
      file.parentId = parentFile.id;
      file.name = javaFile.getName();
      return stateManager.updateFileByFile(file, File.PARENTID | File.NAME);
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  /**
   * Remove the indicated filename from namespace. If the filename
   * is a directory (non empty) and recursive is set to false then throw exception.
   */
  public boolean delete(String src, boolean recursive) throws IOException {
    boolean status = deleteInternal(src, recursive, true);
    if (status && auditLog.isInfoEnabled() && isExternalInvocation()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(), "delete", src, null, null);
    }
    return status;
  }

  /**
   * Remove the indicated filename from the namespace. This may
   * invalidate some blocks that make up the file.
   */
  boolean deleteInternal(String src, boolean recursive, boolean enforcePermission) throws IOException {
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("DIR* NameSystem.delete: " + src);
    }
    if (isInSafeMode()) throw new SafeModeException("Cannot delete " + src, safeMode);
    // delete file; block will be deleted for no file is found on next block report
    Integer lockid = getLock(src.getBytes(), true);
    try {
      return !stateManager.deleteFileByPath(src, recursive).isEmpty();
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  void removePathAndBlocks(String src, List<Block> blocks) throws IOException {
    // avoid compile error
  }

  /**
   * Get the file info for a specific file.
   * 
   * @param src
   *          The string representation of the path to the file
   * @throws IOException
   *           if permission to access file is denied by the system
   * @return object containing information regarding the file
   *         or null if file not found
   */
  HdfsFileStatus getFileInfo(String src) throws IOException {
    return stateManager.getFileInfo(src);
  }

  /**
   * Create all the necessary directories
   */
  public boolean mkdirs(String src, PermissionStatus permissions) throws IOException {
    File file = mkdirsInternal(src, permissions);
    if (auditLog.isInfoEnabled() && isExternalInvocation()) {
      final HdfsFileStatus stat = StateManager.adfsFileToHdfsFileStatus(file);
      logAuditEvent(UserGroupInformation.getCurrentUser(), Server.getRemoteIp(), "mkdirs", src, null, stat);
    }
    return true;
  }

  /**
   * Create all the necessary directories
   */
  private File mkdirsInternal(String src, PermissionStatus permissions) throws IOException {
    NameNode.stateChangeLog.debug("DIR* NameSystem.mkdirs: " + src);
    if (!DFSUtil.isValidName(src)) { throw new IOException("Invalid directory name: " + src); }
    if (isInSafeMode()) throw new SafeModeException("Cannot create directory " + src, safeMode);
    checkFsObjectLimit();
    Integer lockid = getLock(src.getBytes(), true);
    try {
      return stateManager.insertFileByPath(src, 0, -1, (byte) 0, false, null);
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  ContentSummary getContentSummary(String src) throws IOException {
    List<File> fileList = stateManager.findFileDescendantByPath(src, false, true);
    long totalLength = 0;
    long totalDirectoryCount = 0;
    long totalFileCount = 0;
    for (File file : fileList) {
      totalLength += file.length;
      if (file.isDir()) ++totalDirectoryCount;
      else ++totalFileCount;
    }
    return new ContentSummary(totalLength, totalFileCount, totalDirectoryCount);
  }

  /**
   * Set the namespace quota and diskspace quota for a directory.
   * See {@link ClientProtocol#setQuota(String, long, long)} for the
   * contract.
   */
  void setQuota(String path, long nsQuota, long dsQuota) throws IOException {
    // TODO:adfs
  }

  /**
   * Persist all metadata about this file.
   * 
   * @param src
   *          The string representation of the path
   * @param clientName
   *          The string representation of the client
   * @throws IOException
   *           if path does not exist
   */
  void fsync(String src, String clientName) throws IOException {
    // noting to do for adfs
  }

  /**
   * Move a file that is being written to be immutable.
   * 
   * @param src
   *          The filename
   * @param lease
   *          The lease for the client creating the file
   */
  public void internalReleaseLeaseOne(File file, String holder) throws IOException {
    LOG.info("Recovering lease for " + file);
    if (file == null) {
      final String message =
          "DIR* NameSystem.internalReleaseCreate: attempt to release lease for null file from " + holder;
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }
    if (!file.isUnderConstruction()) {
      final String message =
          "DIR* NameSystem.internalReleaseCreate: attempt to release a commite " + file + " from " + holder;
      NameNode.stateChangeLog.warn(message);
      throw new IOException(message);
    }

    List<BlockEntry> blockEntryList = stateManager.getBlockEntryListByFileId(file.id);
    BlockEntry lastBlockEntry = BlockEntry.getLastBlockEntry(blockEntryList);
    DatanodeDescriptor[] targets =
        stateManager.getDatanodeDescriptorArrayByBlockList(lastBlockEntry == null ? null : lastBlockEntry
            .getBlockList(true));

    // Initialize lease recovery for pendingFile. If there are no blocks associated with this file,
    // then reap lease immediately. Otherwise renew the lease and trigger lease recovery.
    if (targets == null || targets.length == 0) {
      finalizeINodeFileUnderConstruction(file, blockEntryList);
      NameNode.stateChangeLog.warn("BLOCK* internalReleaseLease: No blocks found, commit " + file + " from " + holder);
      return;
    }

    // start lease recovery of the last block for this file. assign a random alive data node as the primary data node
    int randomIndex = (int) now();
    for (int i = 0; i < targets.length; i++) {
      int j = (i + randomIndex) % targets.length;
      if (targets[j].isAlive) {
        DatanodeDescriptor primary = targets[j];
        primary.addBlockToBeRecovered(lastBlockEntry.getHdfsBlock(), targets);
        NameNode.stateChangeLog.info("BLOCK* " + lastBlockEntry + " recovery started, primary=" + primary + " from "
            + holder);
        return;
      }
    }
    file.leaseHolder = HdfsConstants.NN_RECOVERY_LEASEHOLDER;
    stateManager.updateFileByFile(file, File.LEASEHOLDER);
    stateManager.renewLease(file.leaseHolder);
  }

  private void finalizeINodeFileUnderConstruction(File file, List<BlockEntry> blockEntryList) throws IOException {
    file.length = BlockEntry.getTotalLength(blockEntryList);
    file.leaseHolder = null;
    stateManager.updateFileByFile(file, File.LENGTH | File.LEASEHOLDER);
    NameNode.stateChangeLog.info("commit " + file);
    checkReplicationFactor(file, blockEntryList);
  }

  public void commitBlockSynchronization(Block lastblock, long newgenerationstamp, long newlength, boolean closeFile,
      boolean deleteblock, DatanodeID[] newtargets) throws IOException {
    LOG.info("commitBlockSynchronization(lastblock=" + lastblock + ", newgenerationstamp=" + newgenerationstamp
        + ", newlength=" + newlength + ", newtargets=" + Arrays.asList(newtargets) + ", closeFile=" + closeFile
        + ", deleteBlock=" + deleteblock + ")");

    if (isInSafeMode()) { throw new SafeModeException("Cannot commitBlockSynchronization " + lastblock, safeMode); }
    BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(lastblock.getBlockId());
    if (blockEntry == null) throw new IOException("Block (=" + lastblock + ") not found");
    File file = stateManager.findFileById(lastblock.getBlockId());
    if (file == null) throw new IOException("file (=" + file + ") not found");
    if (!file.isUnderConstruction()) { throw new IOException("Unexpected block (=" + lastblock + ") since the file (="
        + file + ") is not under construction"); }

    Integer lockid = getLock(file.path.getBytes(), true);
    try {
      if (deleteblock) {
        stateManager.deleteBlockById(lastblock.getBlockId());
      } else {
        com.taobao.adfs.block.Block block = new com.taobao.adfs.block.Block();
        block.id = blockEntry.getBlockId();
        block.fileId = blockEntry.getFileId();
        block.fileIndex = blockEntry.getFileIndex();
        block.length = blockEntry.getLength();
        block.generationStamp = blockEntry.getGenerationStamp();
        // insert a flag block
        if (blockEntry.getBlock(Datanode.NULL_DATANODE_ID) == null) {
          block.datanodeId = Datanode.NULL_DATANODE_ID;
          stateManager.insertBlockByBlock(block);
        }
        // delete old blocks
        for (com.taobao.adfs.block.Block oldBlock : blockEntry.getBlockList(false)) {
          if (oldBlock == null || oldBlock.datanodeId == Datanode.NULL_DATANODE_ID) continue;
          stateManager.deleteBlockByIdAndDatanodeId(blockEntry.getBlockId(), oldBlock.datanodeId);
        }
        // add the block if DatanodeDescriptor valid
        boolean isNewBlockAdded = false;
        for (int i = 0; i < newtargets.length; i++) {
          DatanodeDescriptor node = stateManager.getDatanodeDescriptorByDatanodeId(newtargets[i].getId());
          if (node != null) {
            block.datanodeId = node.getId();
            stateManager.insertBlockByBlock(block);
            isNewBlockAdded = true;
            LOG.info("commitBlockSynchronization(lastblock=" + lastblock + ", newtargets=" + newtargets
                + ") successful");
          } else {
            LOG.error("commitBlockSynchronization included a target DN " + newtargets[i]
                + " which is not known to NN. Ignoring.");
          }
        }
        if (isNewBlockAdded) {
          stateManager.deleteBlockByIdAndDatanodeId(lastblock.getBlockId(), Datanode.NULL_DATANODE_ID);
        }
      }

      // remove lease and close file
      List<BlockEntry> blockEntryList = stateManager.getBlockEntryListByFileId(file.id);
      finalizeINodeFileUnderConstruction(file, blockEntryList);

      LOG.info("commitBlockSynchronization(newblock=" + lastblock + ", file=" + file + ", newgenerationstamp="
          + newgenerationstamp + ", newlength=" + newlength + ", newtargets=" + Arrays.asList(newtargets)
          + ") successful");
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  /**
   * Renew the lease(s) held by the given client
   */
  void renewLease(String holder) throws IOException {
    if (isInSafeMode()) throw new SafeModeException("Cannot renew lease for " + holder, safeMode);
    stateManager.renewLease(holder);
  }

  /**
   * Get a partial listing of the indicated directory
   * 
   * @param src
   *          the directory name
   * @param startAfter
   *          the name to start after
   * @return a partial listing starting after startAfter
   */
  public DirectoryListing getListing(String src, byte[] startAfter) throws IOException {
    HdfsFileStatus[] fileStatusList = stateManager.getListing(src);
    return fileStatusList == null ? null : new DirectoryListing(fileStatusList, 0);
  }

  // ///////////////////////////////////////////////////////
  //
  // These methods are called by datanodes
  //
  // ///////////////////////////////////////////////////////
  /**
   * Register Datanode.
   * <p>
   * The purpose of registration is to identify whether the new datanode serves a new data storage, and will report new
   * data block copies, which the namenode was not aware of; or the datanode is a replacement node for the data storage
   * that was previously served by a different or the same (in terms of host:port) datanode. The data storages are
   * distinguished by their storageIDs. When a new data storage is reported the namenode issues a new unique storageID.
   * <p>
   * Finally, the namenode returns its namespaceID as the registrationID for the datanodes. namespaceID is a persistent
   * attribute of the name space. The registrationID is checked every time the datanode is communicating with the
   * namenode. Datanodes with inappropriate registrationID are rejected. If the namenode stops, and then restarts it can
   * restore its namespaceID and will continue serving the datanodes that has previously registered with the namenode
   * without restarting the whole cluster.
   * 
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode#register()
   */
  public void registerDatanode(DatanodeRegistration nodeReg) throws IOException {
    String dnAddress = Server.getRemoteAddress();
    if (dnAddress == null) {
      // Mostly called inside an RPC.
      // But if not, use address passed by the data-node.
      dnAddress = nodeReg.getHost();
    }

    // check if the datanode is allowed to be connect to the namenode
    if (!verifyNodeRegistration(nodeReg, dnAddress)) { throw new DisallowedDatanodeException(nodeReg); }

    String hostName = nodeReg.getHost();

    // update the datanode's name with ip:port
    DatanodeID dnReg =
        new DatanodeID(dnAddress + ":" + nodeReg.getPort(), nodeReg.getStorageID(), nodeReg.getInfoPort(), nodeReg
            .getIpcPort());
    nodeReg.updateRegInfo(dnReg);
    nodeReg.exportedKeys = getBlockKeys();

    NameNode.stateChangeLog.info("BLOCK* NameSystem.registerDatanode: " + "node registration from " + nodeReg.getName()
        + " storage " + nodeReg.getStorageID());

    DatanodeDescriptor datanodeDescriptor = stateManager.getDatanodeDescriptorByDatanodeId(nodeReg.getId());
    if (datanodeDescriptor == null) {
      datanodeDescriptor = new DatanodeDescriptor(nodeReg, NetworkTopology.DEFAULT_RACK, hostName);
      datanodeDescriptor.updateHeartbeat(0L, 0L, 0L, 0, 0);
    }
    if (nodeReg.getStorageID().equals("")) {
      // this is a new datanode serving a new data storage
      // this data storage has never been registered, it is either empty or was created by pre-storageID version of DFS
      nodeReg.storageID = newStorageID();
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.registerDatanode: " + "new storageID " + nodeReg.getStorageID()
          + " assigned.");
    }
    datanodeDescriptor.isAlive = true;
    datanodeDescriptor.setLastUpdate(now());
    resolveNetworkLocation(datanodeDescriptor);
    stateManager.updateDatanodeByDatanodeDescriptor(datanodeDescriptor);

    if (safeMode != null) {
      safeMode.checkMode();
    }
  }

  /* Resolve a node's network location */
  private void resolveNetworkLocation(DatanodeDescriptor node) {
    List<String> names = new ArrayList<String>(1);
    if (dnsToSwitchMapping instanceof CachedDNSToSwitchMapping) {
      // get the node's IP address
      names.add(node.getHost());
    } else {
      // get the node's host name
      String hostName = node.getHostName();
      int colon = hostName.indexOf(":");
      hostName = (colon == -1) ? hostName : hostName.substring(0, colon);
      names.add(hostName);
    }

    // resolve its network location
    List<String> rName = dnsToSwitchMapping.resolve(names);
    String networkLocation;
    if (rName == null) {
      LOG.error("The resolve call returned null! Using " + NetworkTopology.DEFAULT_RACK + " for host " + names);
      networkLocation = NetworkTopology.DEFAULT_RACK;
    } else {
      networkLocation = rName.get(0);
    }
    node.setNetworkLocation(networkLocation);
  }

  /**
   * Get registrationID for datanodes based on the namespaceID.
   * 
   * @see #registerDatanode(DatanodeRegistration)
   * @see FSImage#newNamespaceID()
   * @return registration ID
   */
  public String getRegistrationID() {
    return Storage.getRegistrationID(getNamespaceInfo());
  }

  /**
   * Generate new storage ID.
   * 
   * @return unique storage ID
   * 
   *         Note: that collisions are still possible if somebody will try
   *         to bring in a data storage from a different cluster.
   * @throws IOException
   */
  private String newStorageID() throws IOException {
    String newID = null;
    while (newID == null) {
      newID = "DS" + Integer.toString(r.nextInt());
      if (stateManager.getDatanodeDescriptorByStorageId(newID) != null) newID = null;
    }
    return newID;
  }

  private boolean isDatanodeDead(DatanodeDescriptor node) {
    return (node.getLastUpdate() < (now() - heartbeatExpireInterval));
  }

  private void setDatanodeDead(DatanodeDescriptor node) throws IOException {
    node.setLastUpdate(0);
  }

  /**
   * The given node has reported in. This method should:
   * 1) Record the heartbeat, so the datanode isn't timed out
   * 2) Adjust usage stats for future block allocation
   * If a substantial amount of time passed since the last datanode heartbeat then request an immediate block report.
   * 
   * @return an array of datanode commands
   * @throws IOException
   */
  DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg, long capacity, long dfsUsed, long remaining,
      int xceiverCount, int xmitsInProgress, int failedVolumes) throws IOException {
    DatanodeCommand cmd = null;
    DatanodeDescriptor nodeinfo = stateManager.getDatanodeDescriptorByDatanodeId(nodeReg.getId());

    // check if this datanode is alive and registered
    if (nodeinfo == null || !nodeinfo.isAlive) return new DatanodeCommand[] { DatanodeCommand.REGISTER };

    // check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(nodeinfo)) {
      setDatanodeDead(nodeinfo);
      stateManager.updateDatanodeByDatanodeDescriptor(nodeinfo);
      throw new DisallowedDatanodeException(nodeinfo);
    }

    // update node info
    nodeinfo.updateHeartbeat(capacity, dfsUsed, remaining, xceiverCount, failedVolumes);
    nodeinfo.setAdminState(AdminStates.NORMAL);
    stateManager.updateDatanodeByDatanodeDescriptor(nodeinfo);

    // check lease recovery
    cmd = nodeinfo.getLeaseRecoveryCommand(Integer.MAX_VALUE);
    if (cmd != null) return new DatanodeCommand[] { cmd };

    // add cmd for datanode
    ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>(3);
    // check pending replication
    cmd = nodeinfo.getReplicationCommand(maxReplicationStreams - xmitsInProgress);
    if (cmd != null) {
      cmds.add(cmd);
    }
    // check block invalidation
    cmd = nodeinfo.getInvalidateBlocks(blockInvalidateLimit);
    if (cmd != null) {
      cmds.add(cmd);
    }
    if (!cmds.isEmpty()) return cmds.toArray(new DatanodeCommand[cmds.size()]);

    // check distributed upgrade
    cmd = getDistributedUpgradeCommand();
    if (cmd != null) { return new DatanodeCommand[] { cmd }; }
    return null;
  }

  /**
   * Periodically calls heartbeatCheck() and updateAccessKey()
   */
  class HeartbeatMonitor implements Runnable {
    private long lastHeartbeatCheck;

    public void run() {
      while (fsRunning) {
        try {
          long now = now();
          if (lastHeartbeatCheck + heartbeatRecheckInterval < now) {
            namenode.getClient().heartbeatCheck();
            lastHeartbeatCheck = now;
          }
        } catch (Exception e) {
          FSNamesystem.LOG.error(StringUtils.stringifyException(e));
        }
        try {
          Thread.sleep(5000); // 5 seconds
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  /**
   * Returns whether or not there were available resources at the last check of
   * resources.
   * 
   * @return true if there were sufficient resources available, false otherwise.
   */
  private boolean nameNodeHasResourcesAvailable() {
    return hasResourcesAvailable;
  }

  /**
   * Perform resource checks and cache the results.
   * 
   * @throws IOException
   */
  private void checkAvailableResources() throws IOException {
    hasResourcesAvailable = nnResourceChecker.hasAvailableDiskSpace();
  }

  /**
   * Periodically calls hasAvailableResources of NameNodeResourceChecker, and if
   * there are found to be insufficient resources available, causes the NN to
   * enter safe mode. If resources are later found to have returned to
   * acceptable levels, this daemon will cause the NN to exit safe mode.
   */
  class NameNodeResourceMonitor implements Runnable {
    @Override
    public void run() {
      try {
        while (fsRunning) {
          checkAvailableResources();
          if (!nameNodeHasResourcesAvailable()) {
            String lowResourcesMsg = "NameNode low on available disk space. ";
            if (!isInSafeMode()) {
              FSNamesystem.LOG.warn(lowResourcesMsg + "Entering safe mode.");
            } else {
              FSNamesystem.LOG.warn(lowResourcesMsg + "Already in safe mode.");
            }
            enterSafeMode(true);
          }
          try {
            Thread.sleep(resourceRecheckInterval);
          } catch (InterruptedException ie) {
            // Deliberately ignore
          }
        }
      } catch (Exception e) {
        FSNamesystem.LOG.error("Exception in NameNodeResourceMonitor: ", e);
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
          Thread.sleep(replicationRecheckInterval);
          namenode.getClient().replicationCheck();
        } catch (InterruptedException ie) {
          LOG.warn("ReplicationMonitor thread received InterruptedException." + ie);
          break;
        } catch (IOException ie) {
          LOG.warn("ReplicationMonitor thread received exception. " + ie + " " + StringUtils.stringifyException(ie));
        } catch (Throwable t) {
          LOG.warn("ReplicationMonitor thread received Runtime exception. " + t + " "
              + StringUtils.stringifyException(t));
          Runtime.getRuntime().exit(-1);
        }
      }
    }
  }

  // ///////////////////////////////////////////////////////
  //
  // These methods are called by the Namenode system, to see
  // if there is any work for registered datanodes.
  //
  // ///////////////////////////////////////////////////////
  /**
   * Compute block replication and block invalidation work
   * that can be scheduled on data-nodes.
   * The datanode will be informed of this work at the next heartbeat.
   * 
   * @return number of blocks scheduled for replication or removal.
   * @throws IOException
   */
  public int computeDatanodeWork() throws IOException {
    int workFound = 0;
    int blocksToProcess = 0;
    int nodesToProcess = 0;
    // blocks should not be replicated or removed if safe mode is on
    if (isInSafeMode()) return workFound;
    blocksToProcess = (int) (numLiveDataNodes() * ReplicationMonitor.REPLICATION_WORK_MULTIPLIER_PER_ITERATION);
    nodesToProcess =
        (int) Math.ceil((double) numLiveDataNodes() * ReplicationMonitor.INVALIDATE_WORK_PCT_PER_ITERATION / 100);

    workFound = computeReplicationWork(blocksToProcess);

    // Update FSNamesystemMetrics counters
    pendingReplicationBlocksCount = pendingReplications.size();
    underReplicatedBlocksCount = neededReplications.size();
    scheduledReplicationBlocksCount = workFound;
    corruptReplicaBlocksCount = corruptReplicas.size();

    workFound += computeInvalidateWork(nodesToProcess);
    return workFound;
  }

  private int computeInvalidateWork(int nodesToProcess) throws IOException {
    int blockCnt = 0;
    for (int nodeCnt = 0; nodeCnt < nodesToProcess; nodeCnt++) {
      int work = invalidateWorkForOneNode();
      if (work == 0) break;
      blockCnt += work;
    }
    return blockCnt;
  }

  /**
   * Scan blocks in {@link #neededReplications} and assign replication
   * work to data-nodes they belong to.
   * 
   * The number of process blocks equals either twice the number of live
   * data-nodes or the number of under-replicated blocks whichever is less.
   * 
   * @return number of blocks scheduled for replication during this iteration.
   */
  private int computeReplicationWork(int blocksToProcess) throws IOException {
    // stall only useful for unit tests (see TestFileAppend4.java)
    if (stallReplicationWork) { return 0; }

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
   * Get a list of block lists to be replicated
   * The index of block lists represents the
   * 
   * @param blocksToProcess
   * @return Return a list of block lists to be replicated.
   *         The block list index represents its replication priority.
   */
  List<List<Block>> chooseUnderReplicatedBlocks(int blocksToProcess) {
    // initialize data structure for the return value
    List<List<Block>> blocksToReplicate = new ArrayList<List<Block>>(UnderReplicatedBlocks.LEVEL);
    for (int i = 0; i < UnderReplicatedBlocks.LEVEL; i++) {
      blocksToReplicate.add(new ArrayList<Block>());
    }

    if (neededReplications.size() == 0) {
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
    blocksToProcess = Math.min(blocksToProcess, neededReplications.size());

    for (int blkCnt = 0; blkCnt < blocksToProcess; blkCnt++, replIndex++) {
      if (!neededReplicationsIterator.hasNext()) {
        // start from the beginning
        replIndex = 0;
        missingBlocksInPrevIter = missingBlocksInCurIter;
        missingBlocksInCurIter = 0;
        blocksToProcess = Math.min(blocksToProcess, neededReplications.size());
        if (blkCnt >= blocksToProcess) break;
        neededReplicationsIterator = neededReplications.iterator();
        assert neededReplicationsIterator.hasNext() : "neededReplications should not be empty.";
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
   * @param block
   *          block to be replicated
   * @param priority
   *          a hint of its priority in the neededReplication queue
   * @return if the block gets replicated or not
   * @throws IOException
   */
  boolean computeReplicationWorkForBlock(Block block, int priority) throws IOException {
    // TODO:adfs to check file lock
    File file = stateManager.findFileByBlockId(block.getBlockId());
    // block should belong to a file abandoned block or block reopened for append
    if (file == null || file.isUnderConstruction()) {
      neededReplications.remove(block, priority); // remove from neededReplications
      replIndex--;
      return false;
    }

    // get a source data-node
    List<DatanodeDescriptor> containingNodes = new ArrayList<DatanodeDescriptor>();
    List<DatanodeDescriptor> containingLiveReplicasNodes = new ArrayList<DatanodeDescriptor>();
    NumberReplicas numReplicas = new NumberReplicas();
    DatanodeDescriptor srcNode = chooseSourceDatanode(block, containingNodes, containingLiveReplicasNodes, numReplicas);
    assert containingLiveReplicasNodes.size() == numReplicas.liveReplicas();
    if ((numReplicas.liveReplicas() + numReplicas.decommissionedReplicas()) <= 0) missingBlocksInCurIter++;
    // block can not be replicated from any node
    if (srcNode == null) return false;

    // do not schedule more if enough replicas is already pending
    int numEffectiveReplicas = numReplicas.liveReplicas() + pendingReplications.getNumReplicas(block);
    if (numEffectiveReplicas >= file.replication) {
      neededReplications.remove(block, priority); // remove from neededReplications
      replIndex--;
      NameNode.stateChangeLog.info("BLOCK* " + "Removing block " + block
          + " from neededReplications as it has enough replicas. numReplicas: " + numReplicas);
      return false;
    }

    // Exclude any nodes that have non-live replicas from the placement. (eg decommissioning or corrupt replicas)
    List<Node> excludedNodes = new ArrayList<Node>();
    for (Node n : containingNodes) {
      if (!containingLiveReplicasNodes.contains(n)) {
        excludedNodes.add(n);
      }
    }

    // choose replication targets: NOT HOLDING THE GLOBAL LOCK
    DatanodeDescriptor targets[] =
        replicator.chooseTarget(file.replication - numEffectiveReplicas, srcNode, containingLiveReplicasNodes,
            excludedNodes, block.getNumBytes());
    if (targets.length == 0) return false;

    // Recheck since global lock was released block should belong to a file abandoned block or block reopened for
    // append
    if (file == null || file.isUnderConstruction()) {
      neededReplications.remove(block, priority); // remove from neededReplications
      replIndex--;
      return false;
    }

    // do not schedule more if enough replicas is already pending
    numReplicas = countNodes(block);
    numEffectiveReplicas = numReplicas.liveReplicas() + pendingReplications.getNumReplicas(block);
    if (numEffectiveReplicas >= file.replication) {
      neededReplications.remove(block, priority); // remove from neededReplications
      replIndex--;
      NameNode.stateChangeLog.info("BLOCK* " + "Removing block " + block
          + " from neededReplications as it has enough replicas.");
      return false;
    }

    // Add block to the to be replicated list
    srcNode.addBlockToBeReplicated(block, targets);

    for (DatanodeDescriptor dn : targets) {
      dn.incBlocksScheduled();
    }

    // Move the block-replication into a "pending" state.
    // The reason we use 'pending' is so we can retry
    // replications that fail after an appropriate amount of time.
    pendingReplications.add(block, targets.length);
    NameNode.stateChangeLog.debug("BLOCK* block " + block + " is moved from neededReplications to pendingReplications");

    // remove from neededReplications
    if (numEffectiveReplicas + targets.length >= file.replication) {
      neededReplications.remove(block, priority); // remove from neededReplications
      replIndex--;
    }
    if (NameNode.stateChangeLog.isInfoEnabled()) {
      StringBuffer targetList = new StringBuffer("datanode(s)");
      for (int k = 0; k < targets.length; k++) {
        targetList.append(' ');
        targetList.append(targets[k].getName());
      }
      NameNode.stateChangeLog.info("BLOCK* ask " + srcNode.getName() + " to replicate " + block + " to " + targetList);
      NameNode.stateChangeLog.debug("BLOCK* neededReplications = " + neededReplications.size()
          + " pendingReplications = " + pendingReplications.size());
    }

    return true;
  }

  /**
   * Parse the data-nodes the block belongs to and choose one,
   * which will be the replication source.
   * 
   * We prefer nodes that are in DECOMMISSION_INPROGRESS state to other nodes
   * since the former do not have write traffic and hence are less busy.
   * We do not use already decommissioned nodes as a source.
   * Otherwise we choose a random node among those that did not reach their
   * replication limit.
   * 
   * In addition form a list of all nodes containing the block
   * and calculate its replication numbers.
   * 
   * @throws IOException
   */
  private DatanodeDescriptor chooseSourceDatanode(Block block, List<DatanodeDescriptor> containingNodes,
      List<DatanodeDescriptor> containingLiveReplicasNodes, NumberReplicas numReplicas) throws IOException {
    containingNodes.clear();
    containingLiveReplicasNodes.clear();
    DatanodeDescriptor srcNode = null;
    int live = 0;
    int decommissioned = 0;
    int corrupt = 0;
    int excess = 0;
    BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(block.getBlockId());
    if (blockEntry == null) return null;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(block);
    Iterator<com.taobao.adfs.block.Block> blockIterator = blockEntry.getBlockList(true).iterator();
    while (blockIterator.hasNext()) {
      DatanodeDescriptor node = stateManager.getDatanodeDescriptorByDatanodeId(blockIterator.next().datanodeId);
      if (node == null) continue;
      Collection<Block> excessBlocks = excessReplicateMap.get(node.getStorageID());
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) corrupt++;
      else if (node.isDecommissionInProgress() || node.isDecommissioned()) decommissioned++;
      else if (excessBlocks != null && excessBlocks.contains(block)) {
        excess++;
      } else {
        containingLiveReplicasNodes.add(node);
        live++;
      }
      containingNodes.add(node);
      // Check if this replica is corrupt
      // If so, do not select the node as src node
      if ((nodesCorrupt != null) && nodesCorrupt.contains(node)) continue;
      // already reached replication limit
      if (node.getNumberOfBlocksToBeReplicated() >= maxReplicationStreams) continue;
      // the block must not be scheduled for removal on srcNode
      if (excessBlocks != null && excessBlocks.contains(block)) continue;
      // never use already decommissioned nodes
      if (node.isDecommissioned()) continue;
      // we prefer nodes that are in DECOMMISSION_INPROGRESS state
      if (node.isDecommissionInProgress() || srcNode == null) {
        srcNode = node;
        continue;
      }
      if (srcNode.isDecommissionInProgress()) continue;
      // switch to a different node randomly
      // this to prevent from deterministically selecting the same node even
      // if the node failed to replicate the block on previous iterations
      if (r.nextBoolean()) srcNode = node;
    }
    if (numReplicas != null) numReplicas.initialize(live, decommissioned, corrupt, excess);
    return srcNode;
  }

  /**
   * Get blocks to invalidate for the first node
   * in {@link #recentInvalidateSets}.
   * 
   * @return number of blocks scheduled for removal during this iteration.
   * @throws IOException
   */
  private int invalidateWorkForOneNode() throws IOException {
    // blocks should not be replicated or removed if safe mode is on
    if (isInSafeMode()) return 0;
    if (recentInvalidateSets.isEmpty()) return 0;
    // get blocks to invalidate for the first node
    String firstNodeId = recentInvalidateSets.keySet().iterator().next();
    assert firstNodeId != null;
    DatanodeDescriptor dn = stateManager.getDatanodeDescriptorByStorageId(firstNodeId);
    if (dn == null) {
      removeFromInvalidates(firstNodeId);
      return 0;
    }

    Collection<Block> invalidateSet = recentInvalidateSets.get(firstNodeId);
    if (invalidateSet == null) return 0;

    ArrayList<Block> blocksToInvalidate = new ArrayList<Block>(blockInvalidateLimit);

    // # blocks that can be sent in one message is limited
    Iterator<Block> it = invalidateSet.iterator();
    for (int blkCount = 0; blkCount < blockInvalidateLimit && it.hasNext(); blkCount++) {
      Block block = it.next();
      block.setDatanodeId(dn.getId());
      blocksToInvalidate.add(block);
      it.remove();
      removeStoredBlock(block, dn);
    }

    // If we send everything in this message, remove this node entry
    if (!it.hasNext()) {
      removeFromInvalidates(firstNodeId);
    }

    dn.addBlocksToBeInvalidated(blocksToInvalidate);

    if (NameNode.stateChangeLog.isInfoEnabled()) {
      StringBuffer blockList = new StringBuffer();
      for (Block blk : blocksToInvalidate) {
        blockList.append(' ');
        blockList.append(blk);
      }
      NameNode.stateChangeLog.info("BLOCK* ask " + dn.getName() + " to delete " + blockList);
    }
    pendingDeletionBlocksCount -= blocksToInvalidate.size();
    return blocksToInvalidate.size();
  }

  public void setNodeReplicationLimit(int limit) {
    this.maxReplicationStreams = limit;
  }

  /**
   * If there were any replication requests that timed out, reap them
   * and put them back into the neededReplication queue
   * 
   * @throws IOException
   */
  void processPendingReplications() throws IOException {
    Block[] timedOutItems = pendingReplications.getTimedOutBlocks();
    if (timedOutItems != null) {
      for (int i = 0; i < timedOutItems.length; i++) {
        NumberReplicas num = countNodes(timedOutItems[i]);
        neededReplications.add(timedOutItems[i], num.liveReplicas(), num.decommissionedReplicas(),
            getReplication(timedOutItems[i]));
      }
      /*
       * If we know the target datanodes where the replication timedout,
       * we could invoke decBlocksScheduled() on it. Its ok for now.
       */
    }
  }

  /**
   * Remove a datanode descriptor.
   * 
   * @throws IOException
   */
  void removeDatanode(DatanodeID nodeID, AdminStates adminStates) throws IOException {
    DatanodeDescriptor node = nodeID == null ? null : stateManager.getDatanodeDescriptorByDatanodeId(nodeID.getId());
    if (node == null) {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.removeDatanode: " + nodeID + " does not exist");
    } else {
      // update datanode info
      if (adminStates != null) node.setAdminState(adminStates);
      if (node.isAlive) node.isAlive = false;
      // process block info
      List<com.taobao.adfs.block.Block> blockListOnThisDatanode =
          stateManager.findBlockByDatanodeId(node.getId(), false);
      if (blockListOnThisDatanode.isEmpty()) {
        if (AdminStates.DEAD_INPROGRESS.equals(node.getAdminState())) node.setAdminState(AdminStates.DEAD);
      } else {
        for (Iterator<com.taobao.adfs.block.Block> it = blockListOnThisDatanode.iterator(); it.hasNext();) {
          com.taobao.adfs.block.Block block = it.next();
          removeStoredBlock(new Block(block), node);
        }
      }
      stateManager.updateDatanodeByDatanodeDescriptor(node);
      unprotectedRemoveDatanode(node);

      if (safeMode != null) {
        safeMode.checkMode();
      }
    }
  }

  void unprotectedRemoveDatanode(DatanodeDescriptor nodeDescr) {
    nodeDescr.resetBlocks();
    removeFromInvalidates(nodeDescr.getStorageID());
    NameNode.stateChangeLog.debug("BLOCK* NameSystem.unprotectedRemoveDatanode: " + nodeDescr.getName()
        + " is out of service now.");
  }

  FSImage getFSImage() {
    return null;
  }

  /**
   * Check if there are any expired heartbeats, and if so,
   * whether any blocks have to be re-replicated.
   * While removing dead datanodes, make sure that only one datanode is marked
   * dead at a time within the synchronized section. Otherwise, a cascading
   * effect causes more datanodes to be declared dead.
   * 
   * @throws IOException
   */
  void heartbeatCheck() throws IOException {
    List<DatanodeDescriptor> datanodeDescriptorList = stateManager.getDatanodeDescriptorList(false);
    for (DatanodeDescriptor node : datanodeDescriptorList) {
      if (node == null) continue;
      if (!AdminStates.DEAD.equals(node.getAdminState()) && isDatanodeDead(node)) {
        NameNode.stateChangeLog.info("BLOCK* NameSystem.heartbeatCheck: lost heartbeat from " + node.getName());
        removeDatanode(node, AdminStates.DEAD_INPROGRESS);
      }
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
  public void processBlocksBeingWrittenReport(DatanodeID nodeID, BlockListAsLongs blocksBeingWritten)
      throws IOException {
    DatanodeDescriptor node = nodeID == null ? null : stateManager.getDatanodeDescriptorByDatanodeId(nodeID.getId());
    if (node == null) throw new IOException("ProcessReport from unregistered node: " + nodeID);

    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(node)) {
      setDatanodeDead(node);
      throw new DisallowedDatanodeException(node);
    }

    for (int i = 0; i < blocksBeingWritten.getNumberOfBlocks(); i++) {
      Block block =
          new Block(blocksBeingWritten.getBlockId(i), blocksBeingWritten.getBlockLen(i), blocksBeingWritten
              .getBlockGenStamp(i));
      BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(block.getBlockId());
      if (blockEntry == null) {
        rejectAddStoredBlock(block, node, "Block not in blockMap with any generation stamp");
        continue;
      }
      File file = stateManager.findFileById(blockEntry.getFileId());
      if (file == null) {
        rejectAddStoredBlock(block, node, "Block does not correspond to any file");
        continue;
      }
      if (!file.isUnderConstruction()) {
        rejectAddStoredBlock(block, node, "Reported as block being written but is a block of closed file.");
        continue;
      }
      BlockEntry lastBlockEntry = stateManager.getLastBlockEntryByFileId(file.id);
      if (lastBlockEntry == null || lastBlockEntry.getBlockId() != block.getBlockId()) {
        rejectAddStoredBlock(block, node, "Reported as block being written but not the last block of "
            + "an under-construction file.");
        continue;
      }

      com.taobao.adfs.block.Block newBlock = new com.taobao.adfs.block.Block();
      newBlock.id = block.getBlockId();
      newBlock.length = block.getNumBytes();
      newBlock.generationStamp = block.getGenerationStamp();
      newBlock.datanodeId = nodeID.getId();
      newBlock.fileId = blockEntry.getFileId();
      newBlock.fileIndex = blockEntry.getFileIndex();
      com.taobao.adfs.block.Block oldBlock = blockEntry.getBlock(nodeID.getId());
      if (oldBlock == null) stateManager.insertBlockByBlock(newBlock);
      else stateManager.updateBlockByBlock(newBlock, com.taobao.adfs.block.Block.ALL);
      if (blockEntry.getBlock(Datanode.NULL_DATANODE_ID) != null) {
        stateManager.deleteBlockByIdAndDatanodeId(newBlock.id, Datanode.NULL_DATANODE_ID);
      }
    }
  }

  /**
   * The given node is reporting all its blocks. Use this info to
   * update the (machine-->blocklist) and (block-->machinelist) tables.
   */
  public void processReport(DatanodeID nodeID, BlockListAsLongs newReport) throws IOException {
    long startTime = now();
    if (newReport == null) newReport = new BlockListAsLongs(new long[0]);
    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: " + "from " + nodeID.getName() + " "
          + newReport.getNumberOfBlocks() + " blocks");
    }
    DatanodeDescriptor node = nodeID == null ? null : stateManager.getDatanodeDescriptorByDatanodeId(nodeID.getId());
    if (node == null || !node.isAlive) throw new IOException("ProcessReport from dead or unknown node: " + nodeID);

    // Check if this datanode should actually be shutdown instead.
    if (shouldNodeShutdown(node)) {
      setDatanodeDead(node);
      throw new DisallowedDatanodeException(node);
    }

    // scan the report and generate the report result
    Collection<Block> toAdd = new LinkedList<Block>();
    Collection<Block> toRemove = new LinkedList<Block>();
    Collection<Block> toInvalidate = new LinkedList<Block>();
    Map<Long, com.taobao.adfs.block.Block> blockMapOnThisDatanode =
        stateManager.findBlockMapByDatanodeId(node.getId(), false);
    Block reportedBlock = new Block(); // a fixed new'ed block to be reused with index i
    for (int i = 0; i < newReport.getNumberOfBlocks(); ++i) {
      reportedBlock.set(newReport.getBlockId(i), newReport.getBlockLen(i), newReport.getBlockGenStamp(i));
      blockMapOnThisDatanode.remove(reportedBlock.getBlockId());
      BlockEntry storedBlockEntry = stateManager.getBlockEntryByBlockId(reportedBlock.getBlockId());
      if (storedBlockEntry == null) {
        // invalidate the reported block if no block is found, it means we cannot find the file by block.fileId
        toInvalidate.add(new Block(reportedBlock, node.getId()));
      } else {
        File file = stateManager.findFileById(storedBlockEntry.getFileId());
        if (file == null) {
          for (com.taobao.adfs.block.Block storedBlock : storedBlockEntry.getBlockList(false)) {
            toRemove.add(new Block(storedBlock));
            if (storedBlock.id != reportedBlock.getBlockId()) toInvalidate.add(new Block(storedBlock));
            else toInvalidate.add(new Block(reportedBlock, storedBlock.datanodeId));
          }
        } else {
          Integer lockid = getLock(file.path.getBytes(), true);
          try {
            int countOfValidBlock = 0;
            // add into toRemove if block on other data node is null or dead
            for (com.taobao.adfs.block.Block storedBlock : storedBlockEntry.getBlockList(false)) {
              if (storedBlock == null || storedBlock.datanodeId == node.getId()) continue;
              DatanodeDescriptor otherNode = stateManager.getDatanodeDescriptorByDatanodeId(storedBlock.datanodeId);
              if (otherNode == null || !otherNode.isAlive) toRemove.add(new Block(storedBlock));
              else if (storedBlock.length >= 0) ++countOfValidBlock;
            }
            // validate the reported block and its replication
            com.taobao.adfs.block.Block storedBlock = storedBlockEntry.getBlock(node.getId());
            if (reportedBlock.getGenerationStamp() < storedBlockEntry.getGenerationStamp()) {
              toInvalidate.add(new Block(reportedBlock, node.getId()));
              if (storedBlock != null) toRemove.add(new Block(storedBlock));
            } else if (reportedBlock.getGenerationStamp() > storedBlockEntry.getGenerationStamp()) {
              // block on name node needs to be updated for smaller generation stamp
              toAdd.add(new Block(reportedBlock, node.getId()));
            } else if (reportedBlock.getGenerationStamp() == storedBlockEntry.getGenerationStamp()) {
              if (reportedBlock.getNumBytes() < storedBlockEntry.getLength()) {
                toInvalidate.add(new Block(reportedBlock, node.getId()));
                if (storedBlock != null) toRemove.add(new Block(storedBlock));
              } else if (reportedBlock.getNumBytes() > storedBlockEntry.getLength()) {
                // block on name node needs to be updated for smaller length
                toAdd.add(new Block(reportedBlock, node.getId()));
              } else if (reportedBlock.getNumBytes() == storedBlockEntry.getLength()) {
                if (storedBlock == null || storedBlock.generationStamp != reportedBlock.getGenerationStamp()
                    || storedBlock.length != reportedBlock.getNumBytes()) {
                  // add or update the block on this data node
                  toAdd.add(new Block(reportedBlock, node.getId()));
                }
                // block on name node is same to the block on data node
                if (++countOfValidBlock != file.replication) {
                  // insert into toAdd to trigger a replication checking if block is over-replicated
                  toAdd.add(new Block(reportedBlock, node.getId()));
                }
              }
            }
          } finally {
            if (lockid != null) {
              releaseFileLock(lockid);
            }
          }
        }
      }
    }
    // collect blocks that have not been reported, so fsck could find corrupted blocks
    Iterator<com.taobao.adfs.block.Block> it = blockMapOnThisDatanode.values().iterator();
    while (it.hasNext()) {
      BlockEntry storedBlockEntry = stateManager.getBlockEntryByBlockId(it.next().id);
      if (storedBlockEntry != null && !storedBlockEntry.getBlockList(false).isEmpty()) {
        // add into toRemve if block on other data node is null or dead
        File file = stateManager.findFileByBlockId(storedBlockEntry.getFileId());
        Integer lockid = null;
        if (file != null) {
          lockid = getLock(file.path.getBytes(), true);
        }
        try {
          for (com.taobao.adfs.block.Block storedBlock : storedBlockEntry.getBlockList(false)) {
            if (storedBlock == null) continue;
            if (file == null) {
              toRemove.add(new Block(storedBlock));
            } else {
              if (storedBlock.datanodeId == node.getId()) {
                if (!file.isUnderConstruction()) toRemove.add(new Block(storedBlock));
              } else {
                DatanodeDescriptor otherNode = stateManager.getDatanodeDescriptorByDatanodeId(storedBlock.datanodeId);
                if (otherNode == null || !otherNode.isAlive) {
                  toRemove.add(new Block(storedBlock));
                }
              }
            }
          }
        } finally {
          if (lockid != null) {
            releaseFileLock(lockid);
          }
        }
      }
    }

    // process report result
    for (Block b : toRemove) {
      DatanodeDescriptor targetNode =
          b.getDatanodeId() == node.getId() ? node : stateManager.getDatanodeDescriptorByDatanodeId(b.getDatanodeId());
      if (NameNode.stateChangeLog.isDebugEnabled())
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: to remove " + b + " on " + targetNode);
      removeStoredBlock(b, targetNode);
    }
    for (Block b : toAdd) {
      DatanodeDescriptor targetNode =
          b.getDatanodeId() == node.getId() ? node : stateManager.getDatanodeDescriptorByDatanodeId(b.getDatanodeId());
      if (NameNode.stateChangeLog.isDebugEnabled())
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.processReport: to add " + b + " on " + targetNode);
      addStoredBlock(b, targetNode, null);
    }
    for (Block b : toInvalidate) {
      DatanodeDescriptor targetNode =
          b.getDatanodeId() == node.getId() ? node : stateManager.getDatanodeDescriptorByDatanodeId(b.getDatanodeId());
      NameNode.stateChangeLog.info("BLOCK* NameSystem.processReport: to invalidate " + b + " on " + targetNode);
      addToInvalidates(b, targetNode, true);
    }
    NameNode.getNameNodeMetrics().blockReport.inc((int) (now() - startTime));
  }

  /**
   * Modify (block-->datanode) map. Remove block from set of
   * needed replications if this takes care of the problem.
   * 
   * @return the block that is stored in blockMap.
   * @throws IOException
   */
  Block addStoredBlock(Block block, DatanodeDescriptor node, DatanodeDescriptor delNodeHint) throws IOException {
    // check block could be added
    BlockEntry storedBlockEntry = stateManager.getBlockEntryByBlockId(block.getBlockId());
    if (storedBlockEntry == null) { return rejectAddStoredBlock(block, node,
        "Block not in blockMap with any generation stamp"); }
    File file = stateManager.findFileById(storedBlockEntry.getFileId());
    if (file == null) return rejectAddStoredBlock(block, node, "Block does not correspond to any file");

    Integer lockid = getLock(file.path.getBytes(), true);
    try {
      // check block is under construction
      boolean blockIsUnderConstruction = false;
      if (file.isUnderConstruction()) {
        BlockEntry lastBlockEntry = stateManager.getLastBlockEntryByFileId(file.id);
        blockIsUnderConstruction = lastBlockEntry.getBlockId() == block.getBlockId();
      }
      // when block is inserted or length is updated from invalid to valid value
      boolean blockIsAdded = false;

      // check new block is corrupt
      if (block.getGenerationStamp() < storedBlockEntry.getGenerationStamp()) {
        LOG.warn("Mark new replica " + block + " from " + node.getName()
            + "as corrupt because its generation statm is less than existing ones");
        markBlockAsCorrupt(block, node, lockid);
      } else if (block.getGenerationStamp() == storedBlockEntry.getGenerationStamp()
          && block.getNumBytes() < storedBlockEntry.getLength()) {
        LOG.warn("Mark new replica " + block + " from " + node.getName()
            + "as corrupt because its length is less than existing ones");
        markBlockAsCorrupt(block, node, lockid);
      } else {
        // new block is valid and add or update it to the data-node
        com.taobao.adfs.block.Block storedBlock = storedBlockEntry.getBlock(node.getId());
        if (storedBlock == null) {
          storedBlock = new com.taobao.adfs.block.Block();
          storedBlock.id = storedBlockEntry.getBlockId();
          storedBlock.fileId = storedBlockEntry.getFileId();
          storedBlock.fileIndex = storedBlockEntry.getFileIndex();
          storedBlock.datanodeId = node.getId();
          storedBlock.length = block.getNumBytes();
          storedBlock.generationStamp = block.getGenerationStamp();
          stateManager.insertBlockByBlock(storedBlock);
          if (storedBlockEntry.onNullDatanode())
            stateManager.deleteBlockByIdAndDatanodeId(storedBlockEntry.getBlockId(), Datanode.NULL_DATANODE_ID);
          if (LOG.isDebugEnabled()) LOG.debug("FSNamesystem.addStoredBlock: add " + block + " from " + storedBlock);
          blockIsAdded = true;
        } else if (storedBlock.generationStamp != block.getGenerationStamp()
            || storedBlock.length != block.getNumBytes()) {
          storedBlock.datanodeId = node.getId();
          storedBlock.generationStamp = block.getGenerationStamp();
          storedBlock.length = block.getNumBytes();
          stateManager.updateBlockByBlock(storedBlock, com.taobao.adfs.block.Block.ALL);
          if (LOG.isDebugEnabled()) LOG.debug("FSNamesystem.addStoredBlock: update " + block + " from " + storedBlock);
          if (storedBlock.length < 0) blockIsAdded = true;
        } else {
          if (LOG.isDebugEnabled()) LOG.debug("FSNamesystem.addStoredBlock: ignore " + block + " from " + storedBlock);
        }
        // process corrupt block on other datanodes
        for (com.taobao.adfs.block.Block storedBlockOnOtherDatanode : storedBlockEntry.getBlockList(false)) {
          if (storedBlockOnOtherDatanode.datanodeId == storedBlock.datanodeId) continue;
          boolean isExistingBlockCorrupt = false;
          if (storedBlockOnOtherDatanode.generationStamp != storedBlock.generationStamp) {
            isExistingBlockCorrupt = true;
          } else if (storedBlockOnOtherDatanode.length != storedBlock.length && !blockIsUnderConstruction) {
            isExistingBlockCorrupt = true;
          }
          if (isExistingBlockCorrupt) {
            LOG.warn("Mark existing replica " + storedBlockOnOtherDatanode + " from "
                + IpAddress.getIpAndPort(node.getId()) + " as corrupt: storedBlock=" + storedBlock);
            markBlockAsCorrupt(block, node, lockid);
          }
        }
      }

      // check replication

      // if file is being written to, do not check replication-factor. it will be checked when file is closed.
      if (blockIsUnderConstruction) return block;
      // do not handle mis-replicated blocks during startup
      if (isInSafeMode()) return block;
      // get the statistic for this storedBlockEntry
      NumberReplicas num = countNodes(storedBlockEntry);
      int numLiveReplicas = num.liveReplicas();
      int numCurrentReplica = numLiveReplicas + pendingReplications.getNumReplicas(block);
      // handle underReplication/overReplication
      if (numCurrentReplica >= file.replication) {
        neededReplications.remove(block, numCurrentReplica, num.decommissionedReplicas, file.replication);
      } else {
        updateNeededReplications(block, blockIsAdded ? 1 : 0, 0);
      }
      if (numCurrentReplica > file.replication) {
        processOverReplicatedBlock(block, file.replication, node, delNodeHint);
      }
      // If the file replication has reached desired value we can remove any corrupt replicas the block may have
      int corruptReplicasCount = corruptReplicas.numCorruptReplicas(block);
      int numCorruptNodes = num.corruptReplicas();
      if (numCorruptNodes != corruptReplicasCount) {
        LOG.warn("Inconsistent number of corrupt replicas for " + block + "blockMap has " + numCorruptNodes
            + " but corrupt replicas map has " + corruptReplicasCount);
      }
      if ((corruptReplicasCount > 0) && (numLiveReplicas >= file.replication)) invalidateCorruptReplicas(block);
      return block;
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  /**
   * Log a rejection of an addStoredBlock RPC, invalidate the reported block,
   * and return it.
   */
  private Block rejectAddStoredBlock(Block block, DatanodeDescriptor node, String msg) {
    NameNode.stateChangeLog.info("BLOCK* NameSystem.addStoredBlock: " + "addStoredBlock request received for " + block
        + " on " + node.getName() + " size " + block.getNumBytes() + " but was rejected: " + msg);
    addToInvalidates(block, node, true);
    return block;
  }

  /**
   * Invalidate corrupt replicas.
   * <p>
   * This will remove the replicas from the block's location list, add them to {@link #recentInvalidateSets} so that
   * they could be further deleted from the respective data-nodes, and remove the block from corruptReplicasMap.
   * <p>
   * This method should be called when the block has sufficient number of live replicas.
   * 
   * @param blk
   *          Block whose corrupt replicas need to be invalidated
   */
  void invalidateCorruptReplicas(Block blk) {
    Collection<DatanodeDescriptor> nodes = corruptReplicas.getNodes(blk);
    boolean gotException = false;
    if (nodes == null) return;
    // Make a copy of this list, since calling invalidateBlock will modify the original (avoid CME)
    nodes = new ArrayList<DatanodeDescriptor>(nodes);
    NameNode.stateChangeLog.debug("NameNode.invalidateCorruptReplicas: invalidating corrupt replicas on "
        + nodes.size() + "nodes");
    for (Iterator<DatanodeDescriptor> it = nodes.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      try {
        invalidateBlock(blk, node);
      } catch (IOException e) {
        NameNode.stateChangeLog.info("NameNode.invalidateCorruptReplicas " + "error in deleting bad block " + blk
            + " on " + node + e);
        gotException = true;
      }
    }
    // Remove the block from corruptReplicasMap
    if (!gotException) corruptReplicas.removeFromCorruptReplicasMap(blk);
  }

  /**
   * Find how many of the containing nodes are "extra", if any.
   * If there are any extras, call chooseExcessReplicates() to
   * mark them in the excessReplicateMap.
   * 
   * @throws IOException
   */
  private void processOverReplicatedBlock(Block block, short replication, DatanodeDescriptor addedNode,
      DatanodeDescriptor delNodeHint) throws IOException {
    if (addedNode == delNodeHint) {
      delNodeHint = null;
    }
    Collection<DatanodeDescriptor> nonExcess = new ArrayList<DatanodeDescriptor>();
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas.getNodes(block);
    BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(block.getBlockId());
    List<com.taobao.adfs.block.Block> blockList = blockEntry.getBlockList(false);
    for (com.taobao.adfs.block.Block tempBlock : blockList) {
      DatanodeDescriptor cur = stateManager.getDatanodeDescriptorByDatanodeId(tempBlock.datanodeId);
      Collection<Block> excessBlocks = excessReplicateMap.get(cur.getStorageID());
      if (excessBlocks == null || !excessBlocks.contains(block)) {
        if (!cur.isDecommissionInProgress() && !cur.isDecommissioned()) {
          // exclude corrupt replicas
          if (corruptNodes == null || !corruptNodes.contains(cur)) {
            nonExcess.add(cur);
          }
        }
      }
    }
    chooseExcessReplicates(nonExcess, block, replication, addedNode, delNodeHint);
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
  void chooseExcessReplicates(Collection<DatanodeDescriptor> nonExcess, Block b, short replication,
      DatanodeDescriptor addedNode, DatanodeDescriptor delNodeHint) {
    // first form a rack to datanodes map and
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

    // split nodes into two sets, priSet contains nodes on rack with more than one replica
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
    while (nonExcess.size() - replication > 0) {
      DatanodeInfo cur = null;
      long minSpace = Long.MAX_VALUE;

      // check if we can del delNodeHint
      if (firstOne && delNodeHint != null && nonExcess.contains(delNodeHint)
          && (priSet.contains(delNodeHint) || (addedNode != null && !priSet.contains(addedNode)))) {
        cur = delNodeHint;
      } else {
        // regular excessive replica removal
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

      Collection<Block> excessBlocks = excessReplicateMap.get(cur.getStorageID());
      if (excessBlocks == null) {
        excessBlocks = new TreeSet<Block>();
        excessReplicateMap.put(cur.getStorageID(), excessBlocks);
      }
      if (excessBlocks.add(b)) {
        excessBlocksCount++;
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.chooseExcessReplicates: " + "(" + cur.getName() + ", " + b
            + ") is added to excessReplicateMap");
      }

      // The 'excessblocks' tracks blocks until we get confirmation that the datanode has deleted them;
      // the only way we remove them is when we get a "removeBlock" message.
      //
      // The 'invalidate' list is used to inform the datanode the block should be deleted.
      // Items are removed from the invalidate list upon giving instructions to the namenode.
      addToInvalidates(b, cur, false);
      NameNode.stateChangeLog.info("BLOCK* NameSystem.chooseExcessReplicates: " + "(" + cur.getName() + ", " + b
          + ") is added to recentInvalidateSets");
    }
  }

  /**
   * Modify (block-->datanode) map. Possibly generate
   * replication tasks, if the removed block is still valid.
   * 
   * @throws IOException
   */
  void removeStoredBlock(Block block, DatanodeDescriptor node) throws IOException {
    // if block is not in the blockMap just return
    BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(block.getBlockId());
    if (node != null) block.setDatanodeId(node.getId());
    if (blockEntry == null || blockEntry.getBlock(block.getDatanodeId()) == null) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: " + block
          + " has already been removed from node " + IpAddress.getIpAndPort(block.getDatanodeId()));
      return;
    }

    File file = stateManager.findFileById(blockEntry.getFileId());
    if (file != null) {
      Integer lockid = getLock(file.path.getBytes(), true);
      try {
        if (blockEntry.getBlockList(false).size() == 1 && !blockEntry.onNullDatanode()) {
          // if this is the last block in the table, insert a block on
          // NULL_DATANODE_ID
          com.taobao.adfs.block.Block adfsBlock = new com.taobao.adfs.block.Block();
          adfsBlock.id = blockEntry.getBlockId();
          adfsBlock.datanodeId = Datanode.NULL_DATANODE_ID;
          adfsBlock.fileId = blockEntry.getFileId();
          adfsBlock.fileIndex = blockEntry.getFileIndex();
          adfsBlock.length = blockEntry.getLength();
          adfsBlock.generationStamp = blockEntry.getGenerationStamp();
          stateManager.insertBlockByBlock(adfsBlock);
        }
        stateManager.deleteBlockByIdAndDatanodeId(block.getBlockId(), block.getDatanodeId());
        NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: " + block + " from "
            + IpAddress.getIpAndPort(block.getDatanodeId()));
        // It's possible that the block was removed because of a datanode
        // failure.
        // If the block is still valid, check if replication is necessary.
        // In that case, put block on a possibly-will-be-replicated list.

        updateNeededReplications(block, -1, 0);
      } finally {
        if (lockid != null) {
          releaseFileLock(lockid);
        }
      }
    }

    if (node != null) {
      // We've removed a block from a node, so it's definitely no longer in "excess" there.
      Collection<Block> excessBlocks = excessReplicateMap.get(node.getStorageID());
      if (excessBlocks != null) {
        if (excessBlocks.remove(block)) {
          excessBlocksCount--;
          NameNode.stateChangeLog.debug("BLOCK* NameSystem.removeStoredBlock: " + block
              + " is removed from excessBlocks");
          if (excessBlocks.size() == 0) excessReplicateMap.remove(node.getStorageID());
        }
      }
      // Remove the replica from corruptReplicas
      corruptReplicas.removeFromCorruptReplicasMap(block, node);
    }
  }

  /**
   * The given node is reporting that it received a certain block.
   */
  public void blockReceived(DatanodeID nodeID, Block block, String delHint) throws IOException {
    DatanodeDescriptor node = nodeID == null ? null : stateManager.getDatanodeDescriptorByDatanodeId(nodeID.getId());
    if (node == null || !node.isAlive) {
      NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceived: " + block
          + " is received from dead or unregistered node " + nodeID.getName());
      throw new IOException("Got blockReceived message from unregistered or dead node " + nodeID.getName());
    }

    if (NameNode.stateChangeLog.isDebugEnabled()) {
      NameNode.stateChangeLog.debug("BLOCK* NameSystem.blockReceived: " + block + " is received from "
          + nodeID.getName());
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
    if (delHint != null && delHint.length() != 0) {
      delHintNode = stateManager.getDatanodeDescriptorByStorageId(delHint);
      if (delHintNode == null) {
        NameNode.stateChangeLog.warn("BLOCK* NameSystem.blockReceived: " + block
            + " is expected to be removed from an unrecorded node " + delHint);
      }
    }

    // Modify the blocks->datanode map and node's map.
    pendingReplications.remove(block);
    addStoredBlock(block, node, delHintNode);
  }

  public long getMissingBlocksCount() {
    // not locking
    return Math.max(missingBlocksInPrevIter, missingBlocksInCurIter);
  }

  long[] getStats() {
    return new long[] { getCapacityTotal(), getCapacityUsed(), getCapacityRemaining(), this.underReplicatedBlocksCount, this.corruptReplicaBlocksCount, getMissingBlocksCount() };
  }

  /**
   * Total raw bytes including non-dfs used space.
   */
  public long getCapacityTotal() {
    return stateManager.getClusterCapacity();
  }

  /**
   * Total used space by data nodes
   */
  public long getCapacityUsed() {
    return stateManager.getClusterDfsUsed();
  }

  /**
   * Total used space by data nodes as percentage of total capacity
   */
  public float getCapacityUsedPercent() {
    long capacity = getCapacityTotal();
    if (capacity <= 0) return 100;
    else return 100.0f * getCapacityUsed() / capacity;
  }

  /**
   * Total used space by data nodes for non DFS purposes such
   * as storing temporary files on the local file system
   */
  public long getCapacityUsedNonDFS() {
    long nonDFSUsed = getCapacityTotal() - getCapacityRemaining() - getCapacityUsed();
    return nonDFSUsed < 0 ? 0 : nonDFSUsed;
  }

  /**
   * Total non-used raw bytes.
   */
  public long getCapacityRemaining() {
    return stateManager.getClusterRemaining();
  }

  /**
   * Total remaining space by data nodes as percentage of total capacity
   */
  public float getCapacityRemainingPercent() {
    long capacity = getCapacityTotal();
    if (capacity <= 0) return 100;
    else return 100.0f * getCapacityRemaining() / capacity;
  }

  /**
   * Total number of connections.
   */
  public int getTotalLoad() {
    return (int) stateManager.getClusterLoad();
  }

  int getNumberOfDatanodes(DatanodeReportType type) throws IOException {
    return getDatanodeListForReport(type).size();
  }

  private ArrayList<DatanodeDescriptor> getDatanodeListForReport(DatanodeReportType type) throws IOException {
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

    // list required datanode to report
    List<DatanodeDescriptor> datanodeList = stateManager.getDatanodeDescriptorList(false);
    ArrayList<DatanodeDescriptor> nodes = new ArrayList<DatanodeDescriptor>(datanodeList.size() + mustList.size());
    for (Iterator<DatanodeDescriptor> it = datanodeList.iterator(); it.hasNext();) {
      DatanodeDescriptor dn = it.next();
      boolean isDead = isDatanodeDead(dn);
      if ((isDead && listDeadNodes) || (!isDead && listLiveNodes)) {
        nodes.add(dn);
      }
      // Remove any form of the this datanode in include/exclude lists.
      mustList.remove(dn.getName());
      mustList.remove(dn.getHost());
      mustList.remove(dn.getHostName());
    }

    // add unregistered datanode to report
    if (listDeadNodes) {
      for (Iterator<String> it = mustList.keySet().iterator(); it.hasNext();) {
        DatanodeDescriptor dn = new DatanodeDescriptor(new DatanodeID(it.next()));
        dn.setLastUpdate(0);
        nodes.add(dn);
      }
    }

    return nodes;
  }

  public DatanodeInfo[] datanodeReport(DatanodeReportType type) throws IOException {
    ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(type);
    DatanodeInfo[] arr = new DatanodeInfo[results.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = new DatanodeInfo(results.get(i));
    }
    return arr;
  }

  /**
   * Save namespace image.
   * This will save current namespace into fsimage file and empty edits file.
   * Requires superuser privilege and safe mode.
   * 
   * @throws AccessControlException
   *           if superuser privilege is violated.
   * @throws IOException
   *           if
   */
  void saveNamespace() throws AccessControlException, IOException {
  }

  /**
   * @throws IOException
   */
  public void DFSNodesStatus(ArrayList<DatanodeDescriptor> live, ArrayList<DatanodeDescriptor> dead) {
    try {
      ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(DatanodeReportType.ALL);
      for (Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
        DatanodeDescriptor node = it.next();
        if (isDatanodeDead(node)) dead.add(node);
        else live.add(node);
      }
    } catch (IOException e) {
      LOG.error(e);
    }
  }

  /**
   * Start decommissioning the specified datanode.
   */
  private void startDecommission(DatanodeDescriptor node) throws IOException {
    if (!node.isDecommissionInProgress() && !node.isDecommissioned()) {
      LOG.info("Start Decommissioning node " + node.getName());
      node.startDecommission();
      stateManager.updateDatanodeByDatanodeDescriptor(node);
      node.decommissioningStatus.setStartTime(now());
      // all the blocks that reside on this node have to be replicated.
      checkDecommissionStateInternal(node);
    }
  }

  /**
   * Stop decommissioning the specified datanodes.
   */
  public void stopDecommission(DatanodeDescriptor node) throws IOException {
    LOG.info("Stop Decommissioning node " + node.getName());
    node.stopDecommission();
    stateManager.updateDatanodeByDatanodeDescriptor(node);
  }

  /**
   * @deprecated use {@link NameNode#getNameNodeAddress()} instead.
   */
  @Deprecated
  public InetSocketAddress getDFSNameNodeAddress() {
    return nameNodeAddress;
  }

  /**
   */
  public Date getStartTime() {
    return new Date(systemStart);
  }

  short getMaxReplication() {
    return (short) maxReplication;
  }

  short getMinReplication() {
    return (short) minReplication;
  }

  short getDefaultReplication() {
    return (short) defaultReplication;
  }

  public void stallReplicationWork() {
    stallReplicationWork = true;
  }

  public void restartReplicationWork() {
    stallReplicationWork = false;
  }

  /**
   * A immutable object that stores the number of live replicas and
   * the number of decommissined Replicas.
   */
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

    public String toString() {
      return "live=" + liveReplicas + " decomissioned=" + decommissionedReplicas + " corrupt=" + corruptReplicas
          + " excess=" + excessReplicas;
    }
  }

  /**
   * Counts the number of nodes in the given list into active and
   * decommissioned counters.
   * 
   * @throws IOException
   */
  public NumberReplicas countNodes(BlockEntry blockEntry) throws IOException {
    // TODO: adfs
    int decommissioned = 0;
    int live = 0;
    int corrupt = 0;
    int excess = 0;
    Collection<DatanodeDescriptor> nodesCorrupt = corruptReplicas.getNodes(blockEntry.getHdfsBlock());
    List<com.taobao.adfs.block.Block> blockList = blockEntry.getBlockList(false);
    for (com.taobao.adfs.block.Block tempBlock : blockList) {
      DatanodeDescriptor node = stateManager.getDatanodeDescriptorByDatanodeId(tempBlock.datanodeId);
      if (node == null) continue;
      if ((nodesCorrupt != null) && (nodesCorrupt.contains(node))) {
        corrupt++;
      } else if (node.isDecommissionInProgress() || node.isDecommissioned()) {
        decommissioned++;
      } else {
        Collection<Block> blocksExcess = excessReplicateMap.get(node.getStorageID());
        if (blocksExcess != null && blocksExcess.contains(blockEntry.getHdfsBlock())) {
          excess++;
        } else {
          live++;
        }
      }
    }
    return new NumberReplicas(live, decommissioned, corrupt, excess);
  }

  /**
   * Return the number of nodes that are live and decommissioned.
   * 
   * @throws IOException
   */
  NumberReplicas countNodes(Block b) throws IOException {
    // TODO: adfs
    return countNodes(stateManager.getBlockEntryByBlockId(b.getBlockId()));
  }

  private void logBlockReplicationInfo(Block block, DatanodeDescriptor srcNode, NumberReplicas num) throws IOException {
    int curReplicas = num.liveReplicas();
    int curExpectedReplicas = getReplication(block);
    BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(block.getBlockId());
    List<com.taobao.adfs.block.Block> blockList = blockEntry.getBlockList(false);
    StringBuffer nodeList = new StringBuffer();
    for (com.taobao.adfs.block.Block tempBlock : blockList) {
      DatanodeDescriptor node = stateManager.getDatanodeDescriptorByDatanodeId(tempBlock.datanodeId);
      nodeList.append(node.name);
      nodeList.append(" ");
    }

    File file = stateManager.findFileByBlockId(block.getBlockId());
    FSNamesystem.LOG.info("Block: " + block + ", Expected Replicas: " + curExpectedReplicas + ", live replicas: "
        + curReplicas + ", corrupt replicas: " + num.corruptReplicas() + ", decommissioned replicas: "
        + num.decommissionedReplicas() + ", excess replicas: " + num.excessReplicas() + ", Is Open File: "
        + file.isUnderConstruction() + ", Datanodes having this block: " + nodeList + ", Current Datanode: "
        + srcNode.name + ", Is current datanode decommissioning: " + srcNode.isDecommissionInProgress());
  }

  /**
   * Return true if there are any blocks on this node that have not
   * yet reached their replication factor. Otherwise returns false.
   * 
   * @throws IOException
   */
  private boolean isReplicationInProgress(DatanodeDescriptor srcNode) throws IOException {
    boolean status = false;
    int underReplicatedBlocks = 0;
    int decommissionOnlyReplicas = 0;
    int underReplicatedInOpenFiles = 0;
    List<com.taobao.adfs.block.Block> blockListOnThisDatanode =
        stateManager.findBlockByDatanodeId(srcNode.getId(), true);

    for (final Iterator<com.taobao.adfs.block.Block> i = blockListOnThisDatanode.iterator(); i.hasNext();) {
      final com.taobao.adfs.block.Block block = i.next();
      File file = stateManager.findFileByBlockId(block.id);
      if (file != null) {
        BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(block.id);
        NumberReplicas num = countNodes(blockEntry);
        int curReplicas = num.liveReplicas();
        int curExpectedReplicas = getReplication(blockEntry.getHdfsBlock());
        if (curExpectedReplicas > curReplicas) {
          // Log info about one block for this node which needs replication
          if (!status) {
            status = true;
            logBlockReplicationInfo(blockEntry.getHdfsBlock(), srcNode, num);
          }
          underReplicatedBlocks++;
          if ((curReplicas == 0) && (num.decommissionedReplicas() > 0)) {
            decommissionOnlyReplicas++;
          }
          if (file.isUnderConstruction()) {
            underReplicatedInOpenFiles++;
          }

          if (!neededReplications.contains(blockEntry.getHdfsBlock())
              && pendingReplications.getNumReplicas(blockEntry.getHdfsBlock()) == 0) {
            // These blocks have been reported from the datanode
            // after the startDecommission method has been executed. These
            // blocks were in flight when the decommission was started.
            neededReplications.add(blockEntry.getHdfsBlock(), curReplicas, num.decommissionedReplicas(),
                curExpectedReplicas);
          }
        }
      }
    }
    srcNode.decommissioningStatus.set(underReplicatedBlocks, decommissionOnlyReplicas, underReplicatedInOpenFiles);

    return status;
  }

  /**
   * Change, if appropriate, the admin state of a datanode to
   * decommission completed. Return true if decommission is complete.
   * 
   * @throws IOException
   */
  boolean checkDecommissionStateInternal(DatanodeDescriptor node) throws IOException {
    // Check to see if all blocks in this decommissioned node has reached their target replication factor.
    if (node.isDecommissionInProgress()) {
      if (!isReplicationInProgress(node)) {
        node.setDecommissioned();
        stateManager.updateDatanodeByDatanodeDescriptor(node);
        LOG.info("Decommission complete for node " + node.getName());
      }
    }
    if (node.isDecommissioned()) { return true; }
    return false;
  }

  /**
   * Keeps track of which datanodes/ipaddress are allowed to connect to the namenode.
   */
  private boolean inHostsList(DatanodeID node, String ipAddr) {
    Set<String> hostsList = hostsReader.getHosts();
    return (hostsList.isEmpty() || (ipAddr != null && hostsList.contains(ipAddr)) || hostsList.contains(node.getHost())
        || hostsList.contains(node.getName()) || ((node instanceof DatanodeInfo) && hostsList
        .contains(((DatanodeInfo) node).getHostName())));
  }

  private boolean inExcludedHostsList(DatanodeID node, String ipAddr) {
    Set<String> excludeList = hostsReader.getExcludedHosts();
    return ((ipAddr != null && excludeList.contains(ipAddr)) || excludeList.contains(node.getHost())
        || excludeList.contains(node.getName()) || ((node instanceof DatanodeInfo) && excludeList
        .contains(((DatanodeInfo) node).getHostName())));
  }

  /**
   * Rereads the config to get hosts and exclude list file names.
   * Rereads the files to update the hosts and exclude lists. It
   * checks if any of the hosts have changed states:
   * 1. Added to hosts --> no further work needed here.
   * 2. Removed from hosts --> mark AdminState as decommissioned.
   * 3. Added to exclude --> start decommission.
   * 4. Removed from exclude --> stop decommission.
   */
  public void refreshNodes(Configuration conf) throws IOException {
    // Reread the config to get dfs.hosts and dfs.hosts.exclude filenames.
    // Update the file names and refresh internal includes and excludes list
    if (conf == null) conf = new Configuration();
    hostsReader.updateFileNames(conf.get("dfs.hosts", ""), conf.get("dfs.hosts.exclude", ""));
    hostsReader.refresh();
    List<DatanodeDescriptor> datanodeList = stateManager.getDatanodeDescriptorList(false);
    for (Iterator<DatanodeDescriptor> it = datanodeList.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      // Check if not include.
      if (!inHostsList(node, null)) {
        node.setDecommissioned(); // case 2.
        stateManager.updateDatanodeByDatanodeDescriptor(node);
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

  }

  void finalizeUpgrade() throws IOException {
  }

  /**
   * Checks if the node is not on the hosts list. If it is not, then
   * it will be ignored. If the node is in the hosts list, but is also
   * on the exclude list, then it will be decommissioned.
   * Returns FALSE if node is rejected for registration.
   * Returns TRUE if node is registered (including when it is on the
   * exclude list and is being decommissioned).
   */
  private boolean verifyNodeRegistration(DatanodeRegistration nodeReg, String ipAddr) throws IOException {
    if (!inHostsList(nodeReg, ipAddr)) { return false; }
    if (inExcludedHostsList(nodeReg, ipAddr)) {
      DatanodeDescriptor node = stateManager.getDatanodeDescriptorByDatanodeId(nodeReg.getId());
      if (node == null) { throw new IOException("verifyNodeRegistration: unknown datanode " + nodeReg.getName()); }
      if (!checkDecommissionStateInternal(node)) {
        startDecommission(node);
      }
    }
    return true;
  }

  /**
   * Checks if the Admin state bit is DECOMMISSIONED. If so, then
   * we should shut it down.
   * 
   * Returns true if the node should be shutdown.
   */
  private boolean shouldNodeShutdown(DatanodeDescriptor node) {
    return (node.isDecommissioned());
  }

  @Deprecated
  public String randomDataNode() {
    return null;
  }

  public DatanodeDescriptor getRandomDatanode() {
    return replicator.chooseTarget(1, null, null, 0)[0];
  }

  /**
   * SafeModeInfo contains information related to the safe mode.
   * <p>
   * An instance of {@link SafeModeInfo} is created when the name node enters safe mode.
   * <p>
   * During name node startup {@link SafeModeInfo} counts the number of <em>safe blocks</em>, those that have at least
   * the minimal number of replicas, and calculates the ratio of safe blocks to the total number of blocks in the
   * system, which is the size of {@link FSNamesystem#blocksMap}. When the ratio reaches the {@link #threshold} it
   * starts the {@link SafeModeMonitor} daemon in order to monitor whether the safe mode {@link #extension} is passed.
   * Then it leaves safe mode and destroys itself.
   * <p>
   * If safe mode is turned on manually then the number of safe blocks is not tracked because the name node is not
   * intended to leave safe mode automatically in the case.
   * 
   * @see ClientProtocol#setSafeMode(FSConstants.SafeModeAction)
   * @see SafeModeMonitor
   */
  class SafeModeInfo {
    // configuration fields
    /** Safe mode threshold condition %. */
    private double threshold;
    /** Safe mode minimum number of datanodes alive */
    private int datanodeThreshold;
    /** Safe mode extension after the threshold. */
    private int extension;
    /** Min replication required by safe mode. */
    private int safeReplication;

    // internal fields
    /**
     * Time when threshold was reached.
     * 
     * <br>
     * -1 safe mode is off <br>
     * 0 safe mode is on, but threshold is not reached yet
     */
    private long reached = -1;
    /** Total number of blocks. */
    int blockTotal;
    /** Number of safe blocks. */
    private int blockSafe;
    /** Number of blocks needed to satisfy safe mode threshold condition */
    private int blockThreshold;
    /** time of the last status printout */
    private long lastStatusReport = 0;
    /** Was safemode entered automatically because available resources were low. */
    private boolean resourcesLow = false;

    /**
     * Creates SafeModeInfo when the name node enters
     * automatic safe mode at startup.
     * 
     * @param conf
     *          configuration
     */
    SafeModeInfo(Configuration conf) {
      this.threshold = conf.getFloat("dfs.safemode.threshold.pct", 0.95f);
      this.datanodeThreshold = conf.getInt("dfs.safemode.min.datanodes", 0);
      this.extension = conf.getInt("dfs.safemode.extension", 0);
      this.safeReplication = conf.getInt("dfs.replication.min", 1);
      this.blockTotal = 0;
      this.blockSafe = 0;
    }

    /**
     * Creates SafeModeInfo when safe mode is entered manually, or because
     * available resources are low.
     * 
     * The {@link #threshold} is set to 1.5 so that it could never be reached. {@link #blockTotal} is set to -1 to
     * indicate that safe mode is manual.
     * 
     * @see SafeModeInfo
     */
    private SafeModeInfo(boolean resourcesLow) {
      this.threshold = 1.5f; // this threshold can never be reached
      this.datanodeThreshold = Integer.MAX_VALUE;
      this.extension = Integer.MAX_VALUE;
      this.safeReplication = Short.MAX_VALUE + 1; // more than maxReplication
      this.blockTotal = -1;
      this.blockSafe = -1;
      this.reached = -1;
      this.resourcesLow = resourcesLow;
      enter();
      reportStatus("STATE* Safe mode is ON.", true);
    }

    /**
     * Check if safe mode is on.
     * 
     * @return true if in safe mode
     */
    synchronized boolean isOn() {
      try {
        assert isConsistent() : " SafeMode: Inconsistent filesystem state: "
            + "Total num of blocks, active blocks, or " + "total safe blocks don't match.";
      } catch (IOException e) {
        System.err.print(StringUtils.stringifyException(e));
      }
      return this.reached >= 0;
    }

    /**
     * Enter safe mode.
     */
    void enter() {
      this.reached = 0;
    }

    /**
     * Leave safe mode.
     * <p>
     * Switch to manual safe mode if distributed upgrade is required.<br>
     * Check for invalid, under- & over-replicated blocks in the end of startup.
     */
    synchronized void leave(boolean checkForUpgrades) {
      if (checkForUpgrades) {
        // verify whether a distributed upgrade needs to be started
        boolean needUpgrade = false;
        try {
          needUpgrade = startDistributedUpgradeIfNeeded();
        } catch (IOException e) {
          FSNamesystem.LOG.error(StringUtils.stringifyException(e));
        }
        if (needUpgrade) {
          // switch to manual safe mode
          safeMode = new SafeModeInfo(false);
          return;
        }
      }

      long timeInSafemode = now() - systemStart;
      NameNode.stateChangeLog.info("STATE* Leaving safe mode after " + timeInSafemode / 1000 + " secs.");
      NameNode.getNameNodeMetrics().safeModeTime.set((int) timeInSafemode);

      if (reached >= 0) {
        NameNode.stateChangeLog.info("STATE* Safe mode is OFF.");
      }
      reached = -1;
      safeMode = null;
      NameNode.stateChangeLog.info("STATE* Network topology has " + stateManager.clusterMap.getNumOfRacks()
          + " racks and " + stateManager.clusterMap.getNumOfLeaves() + " datanodes");
      NameNode.stateChangeLog.info("STATE* UnderReplicatedBlocks has " + neededReplications.size() + " blocks");
    }

    /**
     * Safe mode can be turned off iff
     * the threshold is reached and
     * the extension time have passed.
     * 
     * @return true if can leave or false otherwise.
     */
    synchronized boolean canLeave() {
      if (reached == 0) return false;
      if (now() - reached < extension) {
        reportStatus("STATE* Safe mode ON.", false);
        return false;
      }
      return !needEnter();
    }

    /**
     * There is no need to enter safe mode
     * if DFS is empty or {@link #threshold} == 0
     */
    boolean needEnter() {
      // TODO: temp for adfs
      int deadDatanodeCount = 0;
      try {
        List<DatanodeDescriptor> datanodeDescriptorList = stateManager.getDatanodeDescriptorList(false);
        for (DatanodeDescriptor datanodeDescriptor : datanodeDescriptorList) {
          if (datanodeDescriptor == null) continue;
          if (AdminStates.NORMAL_INPROGRESS.equals(datanodeDescriptor.getAdminState())
              || AdminStates.DECOMMISSION_INPROGRESS.equals(datanodeDescriptor.getAdminState())
              || AdminStates.ERROR_INPROGRESS.equals(datanodeDescriptor.getAdminState())
              || AdminStates.DEAD_INPROGRESS.equals(datanodeDescriptor.getAdminState())) {
            if (isDatanodeDead(datanodeDescriptor)) {
              ++deadDatanodeCount;
              LOG.info("Safe mode check error for " + datanodeDescriptor.getName());
            }
          }
        }
      } catch (IOException e) {
        return true;
      }
      return deadDatanodeCount > 2;
    }

    /**
     * Check and trigger safe mode if needed.
     */
    private synchronized void checkMode() {
      if (needEnter()) {
        enter();
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // the threshold is reached
      if (!isOn() || // safe mode is off
          extension <= 0 || threshold <= 0) { // don't need to wait
        this.leave(true); // leave safe mode
        return;
      }
      if (reached > 0) { // threshold has already been reached before
        reportStatus("STATE* Safe mode ON.", false);
        return;
      }
      // start monitor
      reached = now();
      smmthread = new Daemon(new SafeModeMonitor());
      smmthread.start();
      reportStatus("STATE* Safe mode extension entered.", true);
    }

    /**
     * Set total number of blocks.
     */
    synchronized void setBlockTotal(int total) {
      this.blockTotal = total;
      this.blockThreshold = (int) (blockTotal * threshold);
      checkMode();
    }

    /**
     * Increment number of safe blocks if current block has
     * reached minimal replication.
     * 
     * @param replication
     *          current replication
     */
    synchronized void incrementSafeBlockCount(short replication) {
      if ((int) replication == safeReplication) this.blockSafe++;
      checkMode();
    }

    /**
     * Decrement number of safe blocks if current block has
     * fallen below minimal replication.
     * 
     * @param replication
     *          current replication
     */
    synchronized void decrementSafeBlockCount(short replication) {
      if (replication == safeReplication - 1) this.blockSafe--;
      checkMode();
    }

    /**
     * Check if safe mode was entered manually or automatically (at startup, or
     * when disk space is low).
     */
    boolean isManual() {
      return extension == Integer.MAX_VALUE && !resourcesLow;
    }

    /**
     * Set manual safe mode.
     */
    void setManual() {
      extension = Integer.MAX_VALUE;
    }

    /**
     * Check if safe mode was entered due to resources being low.
     */
    boolean areResourcesLow() {
      return resourcesLow;
    }

    /**
     * Set that resources are low for this instance of safe mode.
     */
    void setResourcesLow() {
      resourcesLow = true;
    }

    /**
     * A tip on how safe mode is to be turned off: manually or automatically.
     */
    String getTurnOffTip() {
      if (reached < 0) return "Safe mode is OFF.";
      String leaveMsg = "";
      if (areResourcesLow()) {
        leaveMsg = "Resources are low on NN. Safe mode must be turned off manually";
      } else {
        leaveMsg = "Safe mode will be turned off automatically";
      }
      if (isManual()) {
        if (getDistributedUpgradeState())
          return leaveMsg + " upon completion of " + "the distributed upgrade: upgrade progress = "
              + getDistributedUpgradeStatus() + "%";
        leaveMsg = "Use \"hadoop dfsadmin -safemode leave\" to turn safe mode off";
      }
      if (blockTotal < 0) return leaveMsg + ".";

      int numLive = numLiveDataNodes();
      String msg = "";
      if (reached == 0) {
        if (blockSafe < blockThreshold) {
          msg +=
              String.format("The reported blocks %d needs additional %d"
                  + " blocks to reach the threshold %.4f of total blocks %d.", blockSafe, (blockThreshold - blockSafe),
                  threshold, blockTotal);
        }
        if (numLive < datanodeThreshold) {
          if (!"".equals(msg)) {
            msg += "\n";
          }
          msg +=
              String.format("The number of live datanodes %d needs an additional %d live "
                  + "datanodes to reach the minimum number %d.", numLive, datanodeThreshold - numLive,
                  datanodeThreshold);
        }
        msg += " " + leaveMsg;
      } else {
        msg =
            String.format("The reported blocks %d has reached the threshold" + " %.4f of total blocks %d.", blockSafe,
                threshold, blockTotal);

        if (datanodeThreshold > 0) {
          msg +=
              String.format(" The number of live datanodes %d has reached " + "the minimum number %d.", numLive,
                  datanodeThreshold);
        }
        msg += " " + leaveMsg;
      }
      if (reached == 0 || isManual()) { // threshold is not reached or manual
        return msg + ".";
      }
      // extension period is in progress
      return msg + " in " + Math.abs(reached + extension - now()) / 1000 + " seconds.";
    }

    /**
     * Print status every 20 seconds.
     */
    private void reportStatus(String msg, boolean rightNow) {
      long curTime = now();
      if (!rightNow && (curTime - lastStatusReport < 20 * 1000)) return;
      NameNode.stateChangeLog.info(msg + " \n" + getTurnOffTip());
      lastStatusReport = curTime;
    }

    /**
     * Returns printable state of the class.
     */
    public String toString() {
      String resText =
          "Current safe blocks = " + blockSafe + ". Target blocks = " + blockThreshold + " for threshold = %"
              + threshold + ". Minimal replication = " + safeReplication + ".";
      if (reached > 0) resText += " Threshold was reached " + new Date(reached) + ".";
      return resText;
    }

    /**
     * Checks consistency of the class state.
     * This is costly and currently called only in assert.
     */
    boolean isConsistent() throws IOException {
      if (blockTotal == -1 && blockSafe == -1) { return true; // manual safe mode
      }
      int activeBlocks = (int) (stateManager.countBlock() - pendingDeletionBlocksCount);
      return (blockTotal == activeBlocks) || (blockSafe >= 0 && blockSafe <= blockTotal);
    }
  }

  /**
   * Periodically check whether it is time to leave safe mode.
   * This thread starts when the threshold level is reached.
   * 
   */
  class SafeModeMonitor implements Runnable {
    /** interval in msec for checking safe mode: {@value} */
    private static final long recheckInterval = 1000;

    /**
     */
    public void run() {
      while (fsRunning && (safeMode != null && !safeMode.canLeave())) {
        try {
          Thread.sleep(recheckInterval);
        } catch (InterruptedException ie) {
        }
      }
      // leave safe mode and stop the monitor
      try {
        leaveSafeMode(true);
      } catch (SafeModeException es) { // should never happen
        String msg = "SafeModeMonitor may not run during distributed upgrade.";
        assert false : msg;
        throw new RuntimeException(msg, es);
      }
      smmthread = null;
    }
  }

  /**
   * Current system time.
   * 
   * @return current time in msec.
   */
  static long now() {
    return System.currentTimeMillis();
  }

  boolean setSafeMode(SafeModeAction action) throws IOException {
    if (action != SafeModeAction.SAFEMODE_GET) {
      switch (action) {
      case SAFEMODE_LEAVE: // leave safe mode
        leaveSafeMode(false);
        break;
      case SAFEMODE_ENTER: // enter safe mode
        enterSafeMode(false);
        break;
      }
    }
    return isInSafeMode();
  }

  /**
   * Check whether the name node is in safe mode.
   * 
   * @return true if safe mode is ON, false otherwise
   */
  boolean isInSafeMode() {
    if (safeMode == null) return false;
    return safeMode.isOn();
  }

  /**
   * Increment number of blocks that reached minimal replication.
   * 
   * @param replication
   *          current replication
   */
  void incrementSafeBlockCount(int replication) {
    if (safeMode == null) return;
    safeMode.incrementSafeBlockCount((short) replication);
  }

  /**
   * Decrement number of blocks that reached minimal replication.
   * 
   * @throws IOException
   */
  void decrementSafeBlockCount(Block b) throws IOException {
    if (safeMode == null) // mostly true
      return;
    safeMode.decrementSafeBlockCount((short) countNodes(b).liveReplicas());
  }

  /**
   * Set the total number of blocks in the system.
   */
  void setBlockTotal() {
    if (safeMode == null) return;
    // TODO: adfs
    safeMode.setBlockTotal(-1);
  }

  /**
   * Get the total number of blocks in the system.
   */
  public long getBlocksTotal() {
    // TODO:
    try {
      return stateManager.countBlock();
    } catch (IOException e) {
      return -1;
    }
  }

  /**
   * Enter safe mode manually.
   * 
   * @throws IOException
   */
  void enterSafeMode(boolean resourcesLow) throws IOException {
    try {
      safeModeLock.writeLock().lock();
      if (!isInSafeMode()) {
        safeMode = new SafeModeInfo(resourcesLow);
        return;
      }
      if (resourcesLow) {
        safeMode.setResourcesLow();
      }
      safeMode.setManual();
      NameNode.stateChangeLog.info("STATE* Safe mode is ON. " + safeMode.getTurnOffTip());
    } finally {
      safeModeLock.writeLock().unlock();
    }
  }

  /**
   * Leave safe mode.
   * 
   * @throws IOException
   */
  void leaveSafeMode(boolean checkForUpgrades) throws SafeModeException {
    try {
      this.safeModeLock.writeLock().lock();
      if (!isInSafeMode()) {
        NameNode.stateChangeLog.info("STATE* Safe mode is already OFF.");
        return;
      }
      if (getDistributedUpgradeState()) throw new SafeModeException("Distributed upgrade is in progress", safeMode);
      safeMode.leave(checkForUpgrades);
    } finally {
      this.safeModeLock.writeLock().unlock();
    }
  }

  String getSafeModeTip() {
    if (!isInSafeMode()) return "";
    return safeMode.getTurnOffTip();
  }

  // Distributed upgrade manager
  UpgradeManagerNamenode upgradeManager = new UpgradeManagerNamenode();

  UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action) throws IOException {
    return upgradeManager.distributedUpgradeProgress(action);
  }

  UpgradeCommand processDistributedUpgradeCommand(UpgradeCommand comm) throws IOException {
    return upgradeManager.processUpgradeCommand(comm);
  }

  int getDistributedUpgradeVersion() {
    return upgradeManager.getUpgradeVersion();
  }

  UpgradeCommand getDistributedUpgradeCommand() throws IOException {
    return upgradeManager.getBroadcastCommand();
  }

  boolean getDistributedUpgradeState() {
    return upgradeManager.getUpgradeState();
  }

  short getDistributedUpgradeStatus() {
    return upgradeManager.getUpgradeStatus();
  }

  boolean startDistributedUpgradeIfNeeded() throws IOException {
    return upgradeManager.startUpgrade();
  }

  PermissionStatus createFsOwnerPermissions(FsPermission permission) {
    return null;
  }

  /**
   * Check to see if we have exceeded the limit on the number
   * of inodes.
   */
  void checkFsObjectLimit() throws IOException {
    if (maxFsObjects != 0 && maxFsObjects <= stateManager.countFile() + getBlocksTotal()) { throw new IOException(
        "Exceeded the configured number of objects " + maxFsObjects + " in the filesystem."); }
  }

  /**
   * Get the total number of objects in the system.
   */
  long getMaxObjects() {
    return maxFsObjects;
  }

  public long getFilesTotal() {
    try {
      return stateManager.countFile();
    } catch (IOException e) {
      return -1;
    }
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
    return Integer.MAX_VALUE;
  }

  public String getFSState() {
    return isInSafeMode() ? "safeMode" : "Operational";
  }

  private ObjectName mbeanName;

  private ObjectName mxBean = null;

  /**
   * Register the FSNamesystem MBean using the name
   * "hadoop:service=NameNode,name=FSNamesystemState"
   * Register the FSNamesystem MXBean using the name
   * "hadoop:service=NameNode,name=NameNodeInfo"
   */
  void registerMBean(Configuration conf) {
    // We wrap to bypass standard mbean naming convention.
    // This wraping can be removed in java 6 as it is more flexible in
    // package naming for mbeans and their impl.
    StandardMBean bean;
    try {
      myFSMetrics = new FSNamesystemMetrics(conf);
      bean = new StandardMBean(this, FSNamesystemMBean.class);
      mbeanName = MBeanUtil.registerMBean("NameNode", "FSNamesystemState", bean);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
    mxBean = MBeanUtil.registerMBean("NameNode", "NameNodeInfo", this);
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
    if (mbeanName != null) MBeanUtil.unregisterMBean(mbeanName);
    if (mxBean != null) MBeanUtil.unregisterMBean(mxBean);
  }

  /**
   * Number of live data nodes
   * 
   * @return Number of live data nodes
   */
  public int numLiveDataNodes() {
    return (int) stateManager.getClusterLiveDatanode();
  }

  /**
   * Number of dead data nodes
   * 
   * @return Number of dead data nodes
   */
  public int numDeadDataNodes() {
    return (int) stateManager.getClusterDeadDatanode();
  }

  /**
   * Sets the generation stamp for this filesystem
   */
  public void setGenerationStamp(long stamp) {
    // nothing to do for adfs
  }

  /**
   * Gets the generation stamp for this filesystem
   */
  public long getGenerationStamp() {
    return 0;
  }

  /**
   * Verifies that the block is associated with a file that has a lease, file version is used as stamp for adfs
   * 
   * @param block
   *          block
   * @param fromNN
   *          if it is for lease recovery initiated by NameNode
   * @return a new generation stamp
   */
  long nextGenerationStampForBlock(Block block, boolean fromNN) throws IOException {
    if (isInSafeMode()) { throw new SafeModeException("Cannot get nextGenStamp for " + block, safeMode); }
    BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(block.getBlockId());
    if (blockEntry == null || blockEntry.getGenerationStamp() != block.getGenerationStamp()) {
      String msg =
          (blockEntry == null) ? block + " is missing" : block + " has out of date GS " + block.getGenerationStamp()
              + " found " + blockEntry.getGenerationStamp() + ", may already be committed";
      LOG.info(msg);
      throw new IOException(msg);
    }
    File file = stateManager.findFileByBlockId(block.getBlockId());
    if (file == null) {
      String msg = block + " does not belong any file.";
      LOG.info(msg);
      throw new IOException(msg);
    }
    if (!file.isUnderConstruction()) {
      String msg = block + " is already commited, !fileINode.isUnderConstruction().";
      LOG.info(msg);
      throw new IOException(msg);
    }
    // Disallow client-initiated recovery once NameNode initiated lease recovery starts
    // There should always be a lease if the file is under recovery, but be paranoid about NPEs
    if (!fromNN && HdfsConstants.NN_RECOVERY_LEASEHOLDER.equals(file.leaseHolder)) {
      String msg = block + "is being recovered by NameNode, ignoring the request from a client";
      LOG.info(msg);
      throw new IOException(msg);
    }

    boolean expired = now() - file.leaseRecoveryTime > NameNode.LEASE_RECOVER_PERIOD;
    if (!expired) {
      String msg = block + " is already being recovered, ignoring this request.";
      LOG.info(msg);
      throw new IOException(msg);
    }

    Integer lockid = getLock(file.path.getBytes(), true);
    try {
      // update file version and return it as next block generation stamp
      file.leaseRecoveryTime = now();
      file = stateManager.updateFileByFile(file, File.LEASERECOVERYTIME);
      return file.version;
    } finally {
      if (lockid != null) {
        releaseFileLock(lockid);
      }
    }
  }

  void changeLease(String src, String dst, HdfsFileStatus dinfo) throws IOException {
    // avoid compile error for hdfs
  }

  /**
   * Serializes leases.
   */
  void saveFilesUnderConstruction(DataOutputStream out) throws IOException {
    // nothing to do for adfs
  }

  public ArrayList<DatanodeDescriptor> getDecommissioningNodes() throws IOException {
    ArrayList<DatanodeDescriptor> decommissioningNodes = new ArrayList<DatanodeDescriptor>();
    ArrayList<DatanodeDescriptor> results = getDatanodeListForReport(DatanodeReportType.LIVE);
    for (Iterator<DatanodeDescriptor> it = results.iterator(); it.hasNext();) {
      DatanodeDescriptor node = it.next();
      if (node.isDecommissionInProgress()) {
        decommissioningNodes.add(node);
      }
    }
    return decommissioningNodes;
  }

  /**
   * Returns the DelegationTokenSecretManager instance in the namesystem.
   * 
   * @return delegation token secret manager object
   */
  public DelegationTokenSecretManager getDelegationTokenSecretManager() {
    return dtSecretManager;
  }

  /**
   * @param renewer
   * @return Token<DelegationTokenIdentifier>
   * @throws IOException
   */
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer) throws IOException {
    if (isInSafeMode()) { throw new SafeModeException("Cannot issue delegation token", safeMode); }
    if (!isAllowedDelegationTokenOp()) { throw new IOException(
        "Delegation Token can be issued only with kerberos or web authentication"); }
    if (dtSecretManager == null || !dtSecretManager.isRunning()) {
      LOG.warn("trying to get DT with no secret manager running");
      return null;
    }

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    String user = ugi.getUserName();
    Text owner = new Text(user);
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(owner, renewer, realUser);
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(dtId, dtSecretManager);
    long expiryTime = dtSecretManager.getTokenExpiryTime(dtId);
    logGetDelegationToken(dtId, expiryTime);
    return token;
  }

  /**
   * 
   * @param token
   * @return New expiryTime of the token
   * @throws InvalidToken
   * @throws IOException
   */
  public long renewDelegationToken(Token<DelegationTokenIdentifier> token) throws InvalidToken, IOException {
    if (isInSafeMode()) { throw new SafeModeException("Cannot renew delegation token", safeMode); }
    if (!isAllowedDelegationTokenOp()) { throw new IOException(
        "Delegation Token can be renewed only with kerberos or web authentication"); }
    String renewer = UserGroupInformation.getCurrentUser().getShortUserName();
    long expiryTime = dtSecretManager.renewToken(token, renewer);
    DelegationTokenIdentifier id = new DelegationTokenIdentifier();
    ByteArrayInputStream buf = new ByteArrayInputStream(token.getIdentifier());
    DataInputStream in = new DataInputStream(buf);
    id.readFields(in);
    logRenewDelegationToken(id, expiryTime);
    return expiryTime;
  }

  /**
   * 
   * @param token
   * @throws IOException
   */
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> token) throws IOException {
    if (isInSafeMode()) { throw new SafeModeException("Cannot cancel delegation token", safeMode); }
    String canceller = UserGroupInformation.getCurrentUser().getUserName();
    DelegationTokenIdentifier id = dtSecretManager.cancelToken(token, canceller);
    logCancelDelegationToken(id);
  }

  /**
   * @param out
   *          save state of the secret manager
   */
  void saveSecretManagerState(DataOutputStream out) throws IOException {
    dtSecretManager.saveSecretManagerState(out);
  }

  /**
   * @param in
   *          load the state of secret manager from input stream
   */
  void loadSecretManagerState(DataInputStream in) throws IOException {
    dtSecretManager.loadSecretManagerState(in);
  }

  /**
   * Log the getDelegationToken operation to edit logs
   * 
   * @param id
   *          identifer of the new delegation token
   * @param expiryTime
   *          when delegation token expires
   */
  private void logGetDelegationToken(DelegationTokenIdentifier id, long expiryTime) throws IOException {
  }

  /**
   * Log the renewDelegationToken operation to edit logs
   * 
   * @param id
   *          identifer of the delegation token being renewed
   * @param expiryTime
   *          when delegation token expires
   */
  private void logRenewDelegationToken(DelegationTokenIdentifier id, long expiryTime) throws IOException {
  }

  /**
   * Log the cancelDelegationToken operation to edit logs
   * 
   * @param id
   *          identifer of the delegation token being cancelled
   */
  private void logCancelDelegationToken(DelegationTokenIdentifier id) throws IOException {
  }

  /**
   * Log the updateMasterKey operation to edit logs
   * 
   * @param key
   *          new delegation key.
   */
  public void logUpdateMasterKey(DelegationKey key) throws IOException {
  }

  /**
   * 
   * @return true if delegation token operation is allowed
   */
  private boolean isAllowedDelegationTokenOp() throws IOException {
    AuthenticationMethod authMethod = getConnectionAuthenticationMethod();
    if (UserGroupInformation.isSecurityEnabled() && (authMethod != AuthenticationMethod.KERBEROS)
        && (authMethod != AuthenticationMethod.KERBEROS_SSL) && (authMethod != AuthenticationMethod.CERTIFICATE)) { return false; }
    return true;
  }

  /**
   * Returns authentication method used to establish the connection
   * 
   * @return AuthenticationMethod used to establish connection
   * @throws IOException
   */
  private AuthenticationMethod getConnectionAuthenticationMethod() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
    if (authMethod == AuthenticationMethod.PROXY) {
      authMethod = ugi.getRealUser().getAuthenticationMethod();
    }
    return authMethod;
  }

  @Override
  // NameNodeMXBean
  public String getHostName() {
    return this.nameNodeHostName;
  }

  @Override
  // NameNodeMXBean
  public String getVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  // NameNodeMXBean
  public long getUsed() {
    return this.getCapacityUsed();
  }

  @Override
  // NameNodeMXBean
  public long getFree() {
    return this.getCapacityRemaining();
  }

  @Override
  // NameNodeMXBean
  public long getTotal() {
    return this.getCapacityTotal();
  }

  @Override
  // NameNodeMXBean
  public String getSafemode() {
    if (!this.isInSafeMode()) return "";
    return "Safe mode is ON." + this.getSafeModeTip();
  }

  @Override
  // NameNodeMXBean
  public boolean isUpgradeFinalized() {
    return true;
  }

  @Override
  // NameNodeMXBean
  public long getNonDfsUsedSpace() {
    return getCapacityUsedNonDFS();
  }

  @Override
  // NameNodeMXBean
  public float getPercentUsed() {
    return getCapacityUsedPercent();
  }

  @Override
  // NameNodeMXBean
  public float getPercentRemaining() {
    return getCapacityRemainingPercent();
  }

  @Override
  // NameNodeMXBean
  public long getTotalBlocks() {
    return getBlocksTotal();
  }

  @Override
  // NameNodeMXBean
  public long getTotalFiles() {
    return getFilesTotal();
  }

  @Override
  // NameNodeMXBean
  public int getThreads() {
    return ManagementFactory.getThreadMXBean().getThreadCount();
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of live node attribute keys to its values
   */
  @Override
  // NameNodeMXBean
  public String getLiveNodes() {
    final Map<String, Object> info = new HashMap<String, Object>();
    try {
      ArrayList<DatanodeDescriptor> aliveNodeList = getDatanodeListForReport(DatanodeReportType.LIVE);
      for (DatanodeDescriptor node : aliveNodeList) {
        final Map<String, Object> innerinfo = new HashMap<String, Object>();
        innerinfo.put("lastContact", getLastContact(node));
        innerinfo.put("usedSpace", getDfsUsed(node));
        info.put(node.getHostName(), innerinfo);
      }
    } catch (IOException e) {
      LOG.error(e);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of dead node attribute keys to its values
   */
  @Override
  // NameNodeMXBean
  public String getDeadNodes() {
    final Map<String, Object> info = new HashMap<String, Object>();
    try {
      ArrayList<DatanodeDescriptor> deadNodeList = getDatanodeListForReport(DatanodeReportType.DEAD);
      for (DatanodeDescriptor node : deadNodeList) {
        final Map<String, Object> innerinfo = new HashMap<String, Object>();
        innerinfo.put("lastContact", getLastContact(node));
        info.put(node.getHostName(), innerinfo);
      }
    } catch (IOException e) {
      LOG.error(e);
    }
    return JSON.toString(info);
  }

  /**
   * Returned information is a JSON representation of map with host name as the
   * key and value is a map of decomisioning node attribute keys to its values
   */
  @Override
  // NameNodeMXBean
  public String getDecomNodes() {
    final Map<String, Object> info = new HashMap<String, Object>();
    try {
      ArrayList<DatanodeDescriptor> decomNodeList = getDecommissioningNodes();
      for (DatanodeDescriptor node : decomNodeList) {
        final Map<String, Object> innerinfo = new HashMap<String, Object>();
        innerinfo.put("underReplicatedBlocks", node.decommissioningStatus.getUnderReplicatedBlocks());
        innerinfo.put("decommissionOnlyReplicas", node.decommissioningStatus.getDecommissionOnlyReplicas());
        innerinfo.put("underReplicateInOpenFiles", node.decommissioningStatus.getUnderReplicatedInOpenFiles());
        info.put(node.getHostName(), innerinfo);
      }
    } catch (IOException e) {
      LOG.error(e);
    }
    return JSON.toString(info);
  }

  private long getLastContact(DatanodeDescriptor alivenode) {
    return (System.currentTimeMillis() - alivenode.getLastUpdate()) / 1000;
  }

  private long getDfsUsed(DatanodeDescriptor alivenode) {
    return alivenode.getDfsUsed();
  }

  /**
   * If the remote IP for namenode method invokation is null, then the
   * invocation is internal to the namenode. Client invoked methods are invoked
   * over RPC and always have address != null.
   */
  private boolean isExternalInvocation() {
    return Server.getRemoteIp() != null;
  }

  /**
   * Log fsck event in the audit log
   */
  void logFsckEvent(String src, InetAddress remoteAddress) throws IOException {
    if (auditLog.isInfoEnabled()) {
      logAuditEvent(UserGroupInformation.getCurrentUser(), remoteAddress, "fsck", src, null, null);
    }
  }

  /**
   * Return the number of racks over which the given block is replicated.
   * Nodes that are decommissioning (or are decommissioned) and corrupt
   * replicas are not counted.
   * 
   * This is only used from the unit tests in 0.20.
   * 
   * @throws IOException
   */
  int getNumberOfRacks(Block b) throws IOException {
    HashSet<String> rackSet = new HashSet<String>(0);
    Collection<DatanodeDescriptor> corruptNodes = corruptReplicas.getNodes(b);
    BlockEntry blockEntry = stateManager.getBlockEntryByBlockId(b.getBlockId());
    List<com.taobao.adfs.block.Block> blockList = blockEntry.getBlockList(false);
    for (com.taobao.adfs.block.Block tempBlock : blockList) {
      DatanodeDescriptor node = stateManager.getDatanodeDescriptorByDatanodeId(tempBlock.datanodeId);
      if (!node.isDecommissionInProgress() && !node.isDecommissioned()
          && (corruptNodes == null || !corruptNodes.contains(node))) {
        String name = node.getNetworkLocation();
        if (!rackSet.contains(name)) {
          rackSet.add(name);
        }
      }
    }
    return rackSet.size();
  }

  // new member comes from merging of namenode and state-manager

  NameNode namenode = null;
  public StateManager stateManager = null;
  Configuration conf = null;

  public StateManager getStateManager() {
    return stateManager;
  }

  synchronized void markNormalDatanodeAsNormalInprogress() throws IOException {
    List<DatanodeDescriptor> datanodeDescriptorList = stateManager.getDatanodeDescriptorList(false);
    for (DatanodeDescriptor datanodeDescriptor : datanodeDescriptorList) {
      if (datanodeDescriptor == null) continue;
      if (AdminStates.NORMAL.equals(datanodeDescriptor.getAdminState())) {
        datanodeDescriptor.setAdminState(AdminStates.NORMAL_INPROGRESS);
        stateManager.updateDatanodeByDatanodeDescriptor(datanodeDescriptor);
        LOG.info("mark as " + AdminStates.NORMAL_INPROGRESS + " for " + datanodeDescriptor.getName());
      }
    }
  }

  void enterSafeMode() throws IOException {
    try {
      safeModeLock.writeLock().lock();
      if (!isInSafeMode()) safeMode = new SafeModeInfo(conf);
      safeMode.checkMode();
    } finally {
      safeModeLock.writeLock().unlock();
    }
  }

  synchronized public void startMonitor() throws IOException {
    fsRunning = true;
    hbthread = new Daemon(new HeartbeatMonitor());
    hbthread.start();

    lmthread = new Daemon(new StateManager.LeaseMonitor());
    lmthread.start();

    replthread = new Daemon(new ReplicationMonitor());
    replthread.start();

    pendingReplications.startMonitor();

    dnthread =
        new Daemon(new DecommissionManager(this).new Monitor(conf.getInt("dfs.namenode.decommission.interval", 30),
            conf.getInt("dfs.namenode.decommission.nodes.per.interval", 5)));
    dnthread.start();

    namenode.startTrashEmptier(conf);
  }

  synchronized public void stopMonitor() throws IOException {
    fsRunning = false;
    try {
      if (pendingReplications != null) pendingReplications.stopMonitor();
      if (hbthread != null) hbthread.interrupt();
      if (replthread != null) replthread.interrupt();
      if (dnthread != null) dnthread.interrupt();
    } catch (Throwable t) {
      throw new IOException(t);
    } finally {
      // using finally to ensure we also wait for lease daemon
      try {
        if (lmthread != null) {
          lmthread.interrupt();
          lmthread.join(3000);
        }
      } catch (Throwable t) {
        LOG.warn("fail to interrrupt lease manager thread", t);
      }
    }
  }

  public void replicationCheck() throws IOException {
    computeDatanodeWork();
    processPendingReplications();
  }

  public void pendingReplicationCheck() throws IOException {
    if (pendingReplications != null) pendingReplications.getMonitor().pendingReplicationCheck();
  }

  public void decommisionCheck() throws IOException {
    if (dnthread != null) ((DecommissionManager.Monitor) dnthread.getRunnable()).check();
  }

  /**
   * Returns existing file lock if found, otherwise obtains a new file lock and
   * returns it.
   * 
   * @param lockid
   *          requested by the user, or null if the user didn't already hold
   *          lock
   * @param file
   *          the file to lock
   * @param waitForLock
   *          if true, will block until the lock is available, otherwise will
   *          simply return null if it could not acquire the lock.
   * @return lockid or null if waitForLock is false and the lock was
   *         unavailable.
   */
  private Integer getLock(byte[] file, boolean waitForLock) throws IOException {
    return internalObtainRowLock(file, waitForLock);
  }

  /**
   * Obtains or tries to obtain the given file lock.
   * 
   * @param waitForLock
   *          if true, will block until the lock is available. Otherwise, just
   *          tries to obtain the lock and returns null if unavailable.
   */
  private Integer internalObtainRowLock(final byte[] file, boolean waitForLock) throws IOException {
    HashedBytes fileKey = new HashedBytes(file);
    CountDownLatch fileLatch = new CountDownLatch(1);

    // loop until we acquire the file lock (unless !waitForLock)
    while (true) {
      CountDownLatch existingLatch = lockedFiles.putIfAbsent(fileKey, fileLatch);
      if (existingLatch == null) {
        break;
      } else {
        // file already locked
        if (!waitForLock) { return null; }
        try {
          if (!existingLatch.await(this.rowLockWaitDuration, TimeUnit.MILLISECONDS)) { return null; }
        } catch (InterruptedException ie) {
          // Empty
        }
      }
    }

    // loop until we generate an unused lock id
    while (true) {
      Integer lockId = lockIdGenerator.incrementAndGet();
      HashedBytes existingFileKey = lockIds.putIfAbsent(lockId, fileKey);
      if (existingFileKey == null) {
        return lockId;
      } else {
        // lockId already in use, jump generator to a new spot
        lockIdGenerator.set(rand.nextInt());
      }
    }
  }

  /**
   * Used by unit tests.
   * 
   * @param lockid
   * @return file that goes with <code>lockid</code>
   */
  byte[] getFileFromLock(final Integer lockid) {
    HashedBytes fileKey = lockIds.get(lockid);
    return fileKey == null ? null : fileKey.getBytes();
  }

  /**
   * Release the file lock!
   * 
   * @param lockId
   *          The lock ID to release.
   */
  void releaseFileLock(final Integer lockId) {
    HashedBytes fileKey = lockIds.remove(lockId);
    if (fileKey == null) {
      LOG.warn("Release unknown lockId: " + lockId);
      return;
    }
    CountDownLatch fileLatch = lockedFiles.remove(fileKey);
    if (fileLatch == null) {
      LOG.error("Releases row not locked, lockId: " + lockId + " file: " + fileKey);
      return;
    }
    fileLatch.countDown();
  }
}
