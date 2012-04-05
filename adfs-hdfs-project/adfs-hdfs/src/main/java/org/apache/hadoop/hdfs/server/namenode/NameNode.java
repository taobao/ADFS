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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.HDFSPolicyProvider;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockListAsLongs;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.UnregisteredDatanodeException;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.IncorrectVersionException;
import org.apache.hadoop.hdfs.server.common.UpgradeStatusReport;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.hdfs.server.namenode.metrics.NameNodeMetrics;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem.CompleteFileStatus;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager.Lease;
import org.apache.hadoop.hdfs.server.namenode.UnderReplicatedBlocks.BlockIterator;


import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.DisallowedDatanodeException;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
import org.apache.hadoop.http.HttpServer;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.CachedDNSToSwitchMapping;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.ScriptBasedMapping;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.security.authorize.ConfiguredPolicy;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.util.Daemon;
import org.apache.hadoop.util.HostsFileReader;
import org.apache.hadoop.util.PluginDispatcher;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.taobao.adfs.state.StateManager;
import com.taobao.adfs.util.Utilities;

/**********************************************************
 * NameNode serves as both directory namespace manager and
 * "inode table" for the Hadoop DFS.  There is a single NameNode
 * running in any DFS deployment.  (Well, except when there
 * is a second backup/failover NameNode.)
 *
 * The NameNode controls two critical tables:
 *   1)  filename->blocksequence (namespace)
 *   2)  block->machinelist ("inodes")
 *
 * The first table is stored on disk and is very precious.
 * The second table is rebuilt every time the NameNode comes
 * up.
 *
 * 'NameNode' refers to both this class as well as the 'NameNode server'.
 * The 'FSNamesystem' class actually performs most of the filesystem
 * management.  The majority of the 'NameNode' class itself is concerned
 * with exposing the IPC interface and the http server to the outside world,
 * plus some configuration management.
 *
 * NameNode implements the ClientProtocol interface, which allows
 * clients to ask for DFS services.  ClientProtocol is not
 * designed for direct use by authors of DFS client code.  End-users
 * should instead use the org.apache.nutch.hadoop.fs.FileSystem class.
 *
 * NameNode also implements the DatanodeProtocol interface, used by
 * DataNode programs that actually store DFS data blocks.  These
 * methods are invoked repeatedly and automatically by all the
 * DataNodes in a DFS deployment.
 *
 * NameNode also implements the NamenodeProtocol interface, used by
 * secondary namenodes or rebalancing processes to get partial namenode's
 * state, for example partial blocksMap etc.
 **********************************************************/

public class NameNode implements ClientProtocol, DatanodeProtocol,
                                 FSConstants,
                                 RefreshAuthorizationPolicyProtocol {
  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }
  
  public long getProtocolVersion(String protocol, 
                                 long clientVersion) throws IOException { 
    if (protocol.equals(ClientProtocol.class.getName())) {
      return ClientProtocol.versionID; 
    } else if (protocol.equals(DatanodeProtocol.class.getName())){
      return DatanodeProtocol.versionID;
    } else if (protocol.equals(NamenodeProtocol.class.getName())){
      return NamenodeProtocol.versionID;
    } else if (protocol.equals(RefreshAuthorizationPolicyProtocol.class.getName())){
      return RefreshAuthorizationPolicyProtocol.versionID;
    } else {
      throw new IOException("Unknown protocol to name node: " + protocol);
    }
  }
    
  public static final int DEFAULT_PORT = 8020;

  public static final Log LOG = LogFactory.getLog(NameNode.class.getName());
  public static final Log stateChangeLog = LogFactory.getLog("org.apache.hadoop.hdfs.StateChange");
  public FSNamesystem namesystem; // TODO: This should private. Use getNamesystem() instead. 
  /** RPC server */
  private Server server;
  /** RPC server address */
  private InetSocketAddress serverAddress = null;
  /** httpServer */
  private HttpServer httpServer;
  /** HTTP server address */
  private InetSocketAddress httpAddress = null;
  /** only used for testing purposes  */
  private boolean stopRequested = false;
  /** Is service level authorization enabled? */
  private boolean serviceAuthEnabled = false;
  /** Activated plug-ins. */
  private PluginDispatcher<NamenodePlugin> pluginDispatcher;
  
  private Configuration conf = null;
  private String zkPath = null;
  
  /** {@inheritDoc} */
  public long nextGenerationStamp(Block block, boolean fromNN) throws IOException {
    return namesystem.nextGenerationStampForBlock(block, fromNN);
  }
  
  /** {@inheritDoc} */
  public void commitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
      ) throws IOException {
    myMetrics.numBlockSynchronizationOps.inc();
    long start = now();
    namesystem.commitBlockSynchronization(block,
        newgenerationstamp, newlength, closeFile, deleteblock, newtargets);
    myMetrics.blockSynchronization.inc(now()-start);
  }
  
  static NameNodeMetrics myMetrics;

  public FSNamesystem getNamesystem() {
    return namesystem;
  }
  
  public static NameNodeMetrics getNameNodeMetrics() {
    return myMetrics;
  }
  
  public static InetSocketAddress getAddress(String address) {
    return NetUtils.createSocketAddr(address, DEFAULT_PORT);
  }
  
  public static InetSocketAddress getAddress(Configuration conf) {
    URI filesystemURI = FileSystem.getDefaultUri(conf);
    String authority = filesystemURI.getAuthority();
    if (authority == null) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s has no authority.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString()));
    }
    if (!FSConstants.HDFS_URI_SCHEME.equalsIgnoreCase(
        filesystemURI.getScheme())) {
      throw new IllegalArgumentException(String.format(
          "Invalid URI for NameNode address (check %s): %s is not of scheme '%s'.",
          FileSystem.FS_DEFAULT_NAME_KEY, filesystemURI.toString(),
          FSConstants.HDFS_URI_SCHEME));
    }
    return getAddress(authority);
  }
  
  public InetSocketAddress getAddress() throws IOException {
    InetSocketAddress inetSocketAddress = null;
    try {
      inetSocketAddress = NameNode.getAddress(conf);
    } catch (Throwable t) {
      LOG.warn("fail to get address from " + FileSystem.FS_DEFAULT_NAME_KEY + ", exception="+ Utilities.getThrowableStackTrace(t));
    }
    List<InetAddress> localInetAddresses = Utilities.getInetAddressList();
    InetAddress bestInetAddress = Utilities.getInetAddressList().get(0);
    if (inetSocketAddress != null) {
      for (InetAddress inetAddress : localInetAddresses) {
        if(inetSocketAddress.getAddress().equals(inetAddress)) return inetSocketAddress;
      }
      LOG.warn("ip in " + FileSystem.FS_DEFAULT_NAME_KEY + " is not ip of this server, choose ip " + bestInetAddress.getHostAddress());
      return new InetSocketAddress(bestInetAddress.getHostAddress(), inetSocketAddress.getPort());
    }
    LOG.warn("ip in " + FileSystem.FS_DEFAULT_NAME_KEY + " is invalid, choose ip " + bestInetAddress.getHostAddress());
    return new InetSocketAddress(bestInetAddress.getHostAddress(), DEFAULT_PORT);
  }

  public static URI getUri(InetSocketAddress namenode) {
    int port = namenode.getPort();
    String portString = port == DEFAULT_PORT ? "" : (":"+port);
    return URI.create(FSConstants.HDFS_URI_SCHEME + "://" 
        + namenode.getHostName()+portString);
  }

  public static long now() {
    return System.currentTimeMillis();
  }
  
  /**
   * Initialize name-node.
   * 
   * @param conf the configuration
   */
  private void initialize(Configuration conf) throws IOException {
    this.conf = conf;
    InetSocketAddress socAddr = getAddress();
    int handlerCount = conf.getInt("dfs.namenode.handler.count", 10);
    
    // set service-level authorization security policy
    if (serviceAuthEnabled = 
          conf.getBoolean(
            ServiceAuthorizationManager.SERVICE_AUTHORIZATION_CONFIG, false)) {
      PolicyProvider policyProvider = 
        (PolicyProvider)(ReflectionUtils.newInstance(
            conf.getClass(PolicyProvider.POLICY_PROVIDER_CONFIG, 
                HDFSPolicyProvider.class, PolicyProvider.class), 
            conf));
      SecurityUtil.setPolicy(new ConfiguredPolicy(conf, policyProvider));
    }
    
    // create rpc server 
    this.server = RPC.getServer(this, socAddr.getHostName(), socAddr.getPort(),
                                handlerCount, false, conf);

    // The rpc-server port can be ephemeral... ensure we have the correct info
    this.serverAddress = this.server.getListenerAddress(); 
    FileSystem.setDefaultUri(conf, getUri(serverAddress));
    LOG.info("Namenode up at: " + this.serverAddress);
    
    myMetrics = new NameNodeMetrics(conf, this);
    
    keepTouchOnZooKeeper(conf);
    
    this.namesystem = new FSNamesystem(this, conf);
    startHttpServer(conf);
    this.server.start();  //start RPC server   

    pluginDispatcher = PluginDispatcher.createFromConfiguration(
      conf, "dfs.namenode.plugins", NamenodePlugin.class);
    pluginDispatcher.dispatchStart(this);
    
    namesystem.updateDatanodesMap(false);
  }
  
  private void keepTouchOnZooKeeper(Configuration conf) throws IOException{
    zkPath = new StringBuilder(FSConstants.ZOOKEEPER_NAMENODE_GROUP)
                        .append("/")
                        .append(serverAddress.getHostName())
                        .append(":")
                        .append(serverAddress.getPort())
                        .toString();
    ZKClient.getInstance(conf).touch(zkPath, new byte[0], true);
    LOG.info("keep touched with ZOOKEEPER");
  }
  
  /**
   * clean up namenode dirs for test or other use, should be used
   * after namenode is initialized
   * @throws IOException
   */
  private static void cleanupOnZooKeeper(Configuration conf) throws IOException {
    LOG.info("clean up work on ZooKeeper starts");
    
    // set conf to zk client
    ZKClient.getInstance(conf);
    String slash = "/";
    StringBuilder sb = new StringBuilder();
    StringBuilder sber = new StringBuilder();
    
    LOG.info("step 1: clean up lease");
    List<String> leases = ZKClient.getInstance().getChildren(
        FSConstants.ZOOKEEPER_LEASE_HOME, null);
    if (leases != null) {
      for (String lease : leases) {
        sb.append(FSConstants.ZOOKEEPER_LEASE_HOME).append(slash).append(lease);
        List<String> fs = ZKClient.getInstance().getChildren(sb.toString(),
            null);
        if (fs != null) {
          for (String f : fs) {
            sber.append(FSConstants.ZOOKEEPER_LEASE_HOME).append(slash).append(
                lease).append(slash).append(f);
            ZKClient.getInstance().delete(sber.toString(), true);
            sber.delete(0, sber.length());
          }
        }
        ZKClient.getInstance().delete(sb.toString(), false);
        sb.delete(0, sb.length());
      }
    }
    LOG.info("step 2: clean up pending");
    List<String> pendingBlks = ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_PENDING_HOME, null);
    if(pendingBlks != null) {
      for(String pendingBlk : pendingBlks) {
        sb.append(FSConstants.ZOOKEEPER_PENDING_HOME).append(slash).append(pendingBlk);
        ZKClient.getInstance().delete(sb.toString(), false);
        sb.delete(0, sb.length());
      }
    }
    LOG.info("step 3: clean up under");
    String subUnder;
    for(int i = 0; i < 2; i++) {
      subUnder = FSConstants.ZOOKEEPER_UNDER_HOME + slash + i;
      List<String> underBlks = ZKClient.getInstance().getChildren(subUnder, null);
      if(underBlks != null) {
        for(String underBlk : underBlks) {
          sb.append(subUnder).append(slash).append(underBlk);
          ZKClient.getInstance().delete(sb.toString(), false);
          sb.delete(0, sb.length());
        }
      }
    }
    LOG.info("step 4: clean up excess");
    List<String> storages = ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_EXCESS_HOME, null);
    if(storages != null) {
      for(String storage : storages) {
        List<String> blks = ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_EXCESS_HOME +
                                                 slash + storage, null);
        if(blks == null) {
          continue;
        }
        for(String blk : blks) {
          sb.append(FSConstants.ZOOKEEPER_EXCESS_HOME).append(slash).append(storage).append(slash).append(blk);
          ZKClient.getInstance().delete(sb.toString(), true);
          sb.delete(0, sb.length());
        }
      }
    }
    LOG.info("step 5: clean up corrupt");
    List<String> blks = ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_CORRUPT_HOME, null);
    if(blks != null) {
      for(String blk : blks) {
        sb.append(FSConstants.ZOOKEEPER_CORRUPT_HOME).append(slash).append(blk);
        List<String> dns = ZKClient.getInstance().getChildren(sb.toString(), null);
        if(dns != null) {
          for(String dn : dns) {
            sber.append(FSConstants.ZOOKEEPER_CORRUPT_HOME).append(slash).
            append(blk).append(slash).append(dn);
            ZKClient.getInstance().delete(sber.toString(), true);
            sber.delete(0, sber.length());
          }
        }
        ZKClient.getInstance().delete(sb.toString(), false);
        sb.delete(0, sb.length());
      }
    }
    LOG.info("step 6: clean up invalidate");
    List<String> dnStorages = ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_INVALIDATE_HOME, null);
    if(dnStorages != null) {
      for(String storage : dnStorages) {
        sb.append(FSConstants.ZOOKEEPER_INVALIDATE_HOME).append(slash).append(storage);
        List<String> iblks = ZKClient.getInstance().getChildren(sb.toString(), null);
        if(iblks != null) {
          for(String blk : iblks) {
            sber.append(FSConstants.ZOOKEEPER_INVALIDATE_HOME).append(slash).append(storage).append(slash).append(blk);
            ZKClient.getInstance().delete(sber.toString(), true);
            sber.delete(0, sber.length());
          }
        }
        ZKClient.getInstance().delete(sb.toString(), false);
        sb.delete(0, sb.length());
      }
    }
    LOG.info("step 7: clean up file");
    List<String> files = ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_FILE_HOME, null);
    if(files != null) {
      for(String file : files) {
        sb.append(FSConstants.ZOOKEEPER_FILE_HOME).append(slash).append(file);
        ZKClient.getInstance().delete(sb.toString(), false);
        sb.delete(0, sb.length());
      }      
    }
    LOG.info("step 8: clean up genstamp");
    ZKClient.getInstance().setData(FSConstants.ZOOKEEPER_GENSTAMP_HOME, 
        new byte[0]);
    
    LOG.info("clean up work on ZooKeeper completed.");
  }

  private void startHttpServer(Configuration conf) throws IOException {
    String infoAddr = 
      NetUtils.getServerAddress(conf, "dfs.info.bindAddress", 
                                "dfs.info.port", "dfs.http.address");
    InetSocketAddress infoSocAddr = NetUtils.createSocketAddr(infoAddr);
    String infoHost = infoSocAddr.getHostName();
    int infoPort = infoSocAddr.getPort();
    this.httpServer = new HttpServer("hdfs", infoHost, infoPort, 
        infoPort == 0, conf);
    if (conf.getBoolean("dfs.https.enable", false)) {
      boolean needClientAuth = conf.getBoolean("dfs.https.need.client.auth", false);
      InetSocketAddress secInfoSocAddr = NetUtils.createSocketAddr(conf.get(
          "dfs.https.address", infoHost + ":" + 0));
      Configuration sslConf = new Configuration(false);
      sslConf.addResource(conf.get("dfs.https.server.keystore.resource",
          "ssl-server.xml"));
      this.httpServer.addSslListener(secInfoSocAddr, sslConf, needClientAuth);
      // assume same ssl port for all datanodes
      InetSocketAddress datanodeSslPort = NetUtils.createSocketAddr(conf.get(
          "dfs.datanode.https.address", infoHost + ":" + 50475));
      this.httpServer.setAttribute("datanode.https.port", datanodeSslPort
          .getPort());
    }
    this.httpServer.setAttribute("name.node", this);
    this.httpServer.setAttribute("name.node.address", getNameNodeAddress());
    this.httpServer.setAttribute("name.conf", conf);
    this.httpServer.addInternalServlet("fsck", "/fsck", FsckServlet.class);
    //this.httpServer.addInternalServlet("getimage", "/getimage", GetImageServlet.class);
    this.httpServer.addInternalServlet("listPaths", "/listPaths/*", ListPathsServlet.class);
    //this.httpServer.addInternalServlet("data", "/data/*", FileDataServlet.class);
    this.httpServer.start();

    // The web-server port can be ephemeral... ensure we have the correct info
    infoPort = this.httpServer.getPort();
    this.httpAddress = new InetSocketAddress(infoHost, infoPort);
    conf.set("dfs.http.address", infoHost + ":" + infoPort);
    LOG.info("Web-server up at: " + infoHost + ":" + infoPort);
  }

  /**
   * Start NameNode.
   * <p>
   * The name-node can be started with one of the following startup options:
   * <ul> 
   * <li>{@link StartupOption#REGULAR REGULAR} - normal name node startup</li>
   * <li>{@link StartupOption#FORMAT FORMAT} - format name node</li>
   * <li>{@link StartupOption#UPGRADE UPGRADE} - start the cluster  
   * upgrade and create a snapshot of the current file system state</li> 
   * <li>{@link StartupOption#ROLLBACK ROLLBACK} - roll the  
   *            cluster back to the previous state</li>
   * </ul>
   * The option is passed via configuration field: 
   * <tt>dfs.namenode.startup</tt>
   * 
   * The conf will be modified to reflect the actual ports on which 
   * the NameNode is up and running if the user passes the port as
   * <code>zero</code> in the conf.
   * 
   * @param conf  confirguration
   * @throws IOException
   */
  public NameNode(Configuration conf) throws IOException {
    try {
      initialize(conf);
    } catch (IOException e) {
      this.stop();
      throw e;
    }
  }

  /**
   * Wait for service to finish.
   * (Normally, it runs forever.)
   */
  public void join() {
    try {
      this.server.join();
    } catch (InterruptedException ie) {
    }
  }

  /**
   * Stop all NameNode threads and wait for all to finish.
   */
  public void stop() {
    if (stopRequested)
      return;
    stopRequested = true;
    if (pluginDispatcher != null) {
      pluginDispatcher.dispatchStop();
    }
    try {
      if (httpServer != null) httpServer.stop();
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
    }
    if(namesystem != null) namesystem.close();
    if(server != null) server.stop();
    if (myMetrics != null) {
      myMetrics.shutdown();
    }
    if (namesystem != null) {
      namesystem.shutdown();
    }
    try {
      if(zkPath != null) {
        ZKClient.getInstance().delete(zkPath, false, true);
      }
      if(conf.getBoolean("test.close.zookeeper", true)) {
        ZKClient.getInstance().close();
      }
    } catch (IOException e) {
      LOG.error("exception occurs when close zkClient.\n " + e);
    }
  }
  
  /////////////////////////////////////////////////////
  // ClientProtocol
  /////////////////////////////////////////////////////
  /** {@inheritDoc} */
  public LocatedBlocks   getBlockLocations(String src, long offset, long length)
      throws IOException {
    myMetrics.numGetBlockLocations.inc();
    long start = now();
    try {
      return namesystem.getBlockLocations(getClientMachine(), src, offset, length);
    } finally {
      myMetrics.getBlocksLocation.inc(now()-start);
    }
  }
  
  private static String getClientMachine() {
    String clientMachine = Server.getRemoteAddress();
    if (clientMachine == null) {
      clientMachine = "";
    }
    return clientMachine;
  }
  
  /** {@inheritDoc} */
  public void create(String src, 
                     FsPermission masked,
                             String clientName, 
                             boolean overwrite,
                             short replication,
                             long blockSize
                             ) throws IOException {
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.create: file "
                         +src+" for "+clientName+" at "+clientMachine);
    }
    if (!checkPathLength(src)) {
      throw new IOException("create: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    myMetrics.numFilesCreated.inc();
    myMetrics.numCreateFileOps.inc();
    namesystem.startFile(src,
        new PermissionStatus(UserGroupInformation.getCurrentUGI().getUserName(),
            null, masked),
        clientName, clientMachine, overwrite, replication, blockSize);
  }

  /** {@inheritDoc} */
  public LocatedBlock append(String src, String clientName) throws IOException {
    LOG.info("append called"); 
    String clientMachine = getClientMachine();
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* NameNode.append: file "
          +src+" for "+clientName+" at "+clientMachine);
    }
    LocatedBlock info = namesystem.appendFile(src, clientName, clientMachine);
    myMetrics.numFilesAppended.inc();
    return info;
  }
  
  /** {@inheritDoc} */
  public boolean recoverLease(String src, String clientName) throws IOException {
    String clientMachine = getClientMachine();
    return namesystem.recoverLease(src, clientName, clientMachine);
  }
  
  /** {@inheritDoc} */
  public boolean setReplication(String src, 
                                short replication
                                ) throws IOException {
    boolean status = namesystem.setReplication(src, replication);
    myMetrics.numSetReplicationOps.inc();
    return status;
  }
    
  /** {@inheritDoc} */
  public void setPermission(String src, FsPermission permissions
      ) throws IOException {
    namesystem.setPermission(src, permissions);
  }

  /** {@inheritDoc} */
  public void setOwner(String src, String username, String groupname
      ) throws IOException {
    namesystem.setOwner(src, username, groupname);
  }

  /**
   * Stub for 0.20 clients that don't support HDFS-630
   */
  public LocatedBlock addBlock(String src, 
                               String clientName) throws IOException {
    return addBlock(src, clientName, null);
  }

  public LocatedBlock addBlock(String src,
                               String clientName,
                               DatanodeInfo[] excludedNodes)
    throws IOException {
    List<Node> excludedNodeList = null;
    if (excludedNodes != null) {
      // We must copy here, since this list gets modified later on
      // in ReplicationTargetChooser
      excludedNodeList = new ArrayList<Node>(Arrays.<Node>asList(excludedNodes));
    }

    stateChangeLog.debug("*BLOCK* NameNode.addBlock: file "
                         +src+" for "+clientName);
    LocatedBlock locatedBlock = namesystem.getAdditionalBlock(src, clientName, excludedNodeList);
    if (locatedBlock != null)
      myMetrics.numAddBlockOps.inc();
    return locatedBlock;
  }

  /**
   * The client needs to give up on the block.
   */
  public void abandonBlock(Block b, String src, String holder
      ) throws IOException {
    stateChangeLog.debug("*BLOCK* NameNode.abandonBlock: "
                         +b+" of file "+src);
    if (!namesystem.abandonBlock(b, src, holder)) {
      throw new IOException("Cannot abandon block during write to " + src);
    }
  }

  /** {@inheritDoc} */
  public boolean complete(String src, String clientName) throws IOException {
    stateChangeLog.info("*DIR* calling NameNode.complete: " + src + " for " + clientName);
    myMetrics.numFileCompleteOps.inc();
    long start = now();
    CompleteFileStatus returnCode = namesystem.completeFile(src, clientName);
    LOG.info("COMPLETE* file=" + src + ", client=" + clientName +
    		", lastTime(millsec)=" + (now()-start) + ", ret=" + returnCode);
    myMetrics.fileComplete.inc(now()-start);
    if (returnCode == CompleteFileStatus.STILL_WAITING) {
      return false;
    } else if (returnCode == CompleteFileStatus.COMPLETE_SUCCESS) {
      return true;
    } else {
      throw new IOException("Could not complete write to file " + src + " by " + clientName);
    }
  }

  /**
   * The client has detected an error on the specified located blocks 
   * and is reporting them to the server.  For now, the namenode will 
   * mark the block as corrupt.  In the future we might 
   * check the blocks are actually corrupt. 
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    stateChangeLog.info("*DIR* NameNode.reportBadBlocks");
    namesystem.reportBadBlocks(blocks);
  }
  
  public long getPreferredBlockSize(String filename) throws IOException {
    return namesystem.getPreferredBlockSize(filename);
  }
  
  /**
   */
  public boolean rename(String src, String dst) throws IOException {
    stateChangeLog.debug("*DIR* NameNode.rename: " + src + " to " + dst);
    if (!checkPathLength(dst)) {
      throw new IOException("rename: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    boolean ret = namesystem.renameTo(src, dst);
    if (ret) {
      myMetrics.numFilesRenamed.inc();
    }
    return ret;
  }
    
  /**
   */
  @Deprecated
  public boolean delete(String src) throws IOException {
    LOG.info("delete " + src + " called"); 
    return delete(src, true);
  }

  /** {@inheritDoc} */
  public boolean delete(String src, boolean recursive) throws IOException {
    if (stateChangeLog.isDebugEnabled()) {
      stateChangeLog.debug("*DIR* Namenode.delete: src=" + src
          + ", recursive=" + recursive);
    }
    boolean ret = namesystem.delete(src, recursive);
    if (ret) 
      myMetrics.numDeleteFileOps.inc();
    return ret;
  }

  /**
   * Check path length does not exceed maximum.  Returns true if
   * length and depth are okay.  Returns false if length is too long 
   * or depth is too great.
   * 
   */
  private boolean checkPathLength(String src) {
    Path srcPath = new Path(src);
    return (src.length() <= MAX_PATH_LENGTH &&
            srcPath.depth() <= MAX_PATH_DEPTH);
  }
    
  /** {@inheritDoc} */
  public boolean mkdirs(String src, FsPermission masked) throws IOException {
    stateChangeLog.debug("*DIR* NameNode.mkdirs: " + src);
    if (!checkPathLength(src)) {
      throw new IOException("mkdirs: Pathname too long.  Limit " 
                            + MAX_PATH_LENGTH + " characters, " + MAX_PATH_DEPTH + " levels.");
    }
    return namesystem.mkdirs(src, masked);
  }

  /**
   */
  public void renewLease(String clientName) throws IOException {
    namesystem.renewLease(clientName);
    myMetrics.numRenewLeaseOps.inc();
  }

  /**
   */
  public FileStatus[] getListing(String src) throws IOException {
    FileStatus[] files = namesystem.getListing(src);
    if (files != null) {
      myMetrics.numGetListingOps.inc();
    }
    return files;
  }

  /**
   * Get the file info for a specific file.
   * @param src The string representation of the path to the file
   * @throws IOException if permission to access file is denied by the system
   * @return object containing information regarding the file
   *         or null if file not found
   */
  public FileStatus getFileInfo(String src)  throws IOException {
    myMetrics.numFileInfoOps.inc();
    return namesystem.getFileInfo(src);
  }

  /** @inheritDoc */
  public long[] getStats() {
    return namesystem.getStats();
  }
  
  /**
   */
  public DatanodeInfo[] getDatanodeReport(DatanodeReportType type)
  throws IOException {
    DatanodeInfo results[] = namesystem.datanodeReport(type);
    if (results == null ) {
      throw new IOException("Cannot find datanode report");
    }
    return results;
  }
    
  /**
   * @inheritDoc
   */
  public boolean setSafeMode(SafeModeAction action) throws IOException {
    return namesystem.setSafeMode(action);
  }

  /**
   * Is this namenode currently in safe mode?
   */
  public boolean isInSafeMode() {
    return namesystem.safeMode.get();
  }

  /**
   * @inheritDoc
   */
  public void saveNamespace() throws IOException {
    namesystem.saveNamespace();
  }

  /**
   * Refresh the list of datanodes that the namenode should allow to  
   * connect.  Re-reads conf by creating new Configuration object and 
   * uses the files list in the configuration to update the list. 
   */
  public void refreshNodes() throws IOException {
    namesystem.refreshNodes(conf);
  }

  public void finalizeUpgrade() throws IOException {
    namesystem.finalizeUpgrade();
  }

  public UpgradeStatusReport distributedUpgradeProgress(UpgradeAction action
                                                        ) throws IOException {
    return namesystem.distributedUpgradeProgress(action);
  }

  /**
   * Dumps namenode state into specified file
   */
  public void metaSave(String filename) throws IOException {
    namesystem.metaSave(filename);
  }

  /** {@inheritDoc} */
  public ContentSummary getContentSummary(String path) throws IOException {
    return namesystem.getContentSummary(path);
  }
  
  /** {@inheritDoc} */
  public void setQuota(String path, long namespaceQuota, long diskspaceQuota) 
                       throws IOException {
    namesystem.setQuota(path, namespaceQuota, diskspaceQuota);
  }
  
  /** {@inheritDoc} */
  public void fsync(String src, String clientName) throws IOException {
    namesystem.fsync(src, clientName);
    myMetrics.numFsyncOps.inc();
  }

  /** @inheritDoc */
  public void setTimes(String src, long mtime, long atime) throws IOException {
    namesystem.setTimes(src, mtime, atime);
  }

  ////////////////////////////////////////////////////////////////
  // DatanodeProtocol
  ////////////////////////////////////////////////////////////////
  /** 
   */
  public DatanodeRegistration register(DatanodeRegistration nodeReg
                                       ) throws IOException {
    verifyVersion(nodeReg.getVersion());
    namesystem.registerDatanode(nodeReg);
    return nodeReg;
  }
    
  /**
   * Data node notify the name node that it is alive 
   * Return an array of block-oriented commands for the datanode to execute.
   * This will be either a transfer or a delete operation.
   */
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration nodeReg,
                                       long capacity,
                                       long dfsUsed,
                                       long remaining,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException {
    verifyRequest(nodeReg);
    return namesystem.handleHeartbeat(nodeReg, capacity, dfsUsed, remaining,
        xceiverCount, xmitsInProgress);
  }

  public DatanodeCommand blockReport(DatanodeRegistration nodeReg,
                                     long[] blocks) throws IOException {
    verifyRequest(nodeReg);
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    stateChangeLog.info("*BLOCK* NameNode.blockReport: "
           + blist.getNumberOfBlocks() +" blocks from " + nodeReg.getName());
    namesystem.processReport(nodeReg, blist);
    return null;
  }
  
  /**
   * add new replica blocks to the Inode to target mapping
   * also add the Inode file to DataNodeDesc
   */
  public void blocksBeingWrittenReport(DatanodeRegistration nodeReg,
      long[] blocks) throws IOException {
    verifyRequest(nodeReg);
    BlockListAsLongs blist = new BlockListAsLongs(blocks);
    namesystem.processBlocksBeingWrittenReport(nodeReg, blist);
    
    stateChangeLog.info("*BLOCK* NameNode.blocksBeingWrittenReport: "
           +"from "+nodeReg.getName()+" "+blist.getNumberOfBlocks() +" blocks");
    
  }
  
  public void blockReceived(DatanodeRegistration nodeReg, 
                            Block blocks[],
                            String delHints[]) throws IOException {
    verifyRequest(nodeReg);
    stateChangeLog.debug("*BLOCK* NameNode.blockReceived: "
                         +"from "+nodeReg.getName()+" "+blocks.length+" blocks.");
    long start = now();
    for (int i = 0; i < blocks.length; i++) {
      namesystem.blockReceived(nodeReg, blocks[i], delHints[i]);
    }
    myMetrics.numBlockReceivedOps.inc();
    if(blocks.length > 0) {
      myMetrics.blockReceived.inc(blocks.length, now()-start);
    }
  }

  /**
   */
  public void errorReport(DatanodeRegistration nodeReg,
                          int errorCode, 
                          String msg) throws IOException {
    LOG.info("errorReport called"); 
    // Log error message from datanode
    String dnName = (nodeReg == null ? "unknown DataNode" : nodeReg.getName());
    LOG.info("Error report from " + dnName + ": " + msg);
    if (errorCode == DatanodeProtocol.NOTIFY) {
      return;
    }
    verifyRequest(nodeReg);
    if (errorCode == DatanodeProtocol.DISK_ERROR) {
      LOG.warn("Volume failed on " + dnName); 
    } else if (errorCode == DatanodeProtocol.FATAL_DISK_ERROR) {
      namesystem.removeDatanode(nodeReg);            
    }
  }
    
  public NamespaceInfo versionRequest() throws IOException {
    return namesystem.getNamespaceInfo();
  }

  public UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException {
    return namesystem.processDistributedUpgradeCommand(comm);
  }

  /** 
   * Verify request.
   * 
   * Verifies correctness of the datanode version, registration ID, and 
   * if the datanode does not need to be shutdown.
   * 
   * @param nodeReg data node registration
   * @throws IOException
   */
  public void verifyRequest(DatanodeRegistration nodeReg) throws IOException {
    verifyVersion(nodeReg.getVersion());
  }
    
  /**
   * Verify version.
   * 
   * @param version
   * @throws IOException
   */
  public void verifyVersion(int version) throws IOException {
    if (version != LAYOUT_VERSION)
      throw new IncorrectVersionException(version, "data node");
  }

  /**
   * Returns the address on which the NameNodes is listening to.
   * @return the address on which the NameNodes is listening to.
   */
  public InetSocketAddress getNameNodeAddress() {
    return serverAddress;
  }

  /**
   * Returns the address of the NameNodes http server, 
   * which is used to access the name-node web UI.
   * 
   * @return the http address.
   */
  public InetSocketAddress getHttpAddress() {
    return httpAddress;
  }

  NetworkTopology getNetworkTopology() {
    return namesystem.clusterMap;
  }
  
  /** Format a new filesystem.  Destroys any filesystem that may already
   * exist at this location.  **/
  public static void format(Configuration conf) throws IOException {
    format(conf, false);
  }
  
  /**
   * Verify that configured directories exist, then
   * Interactively confirm that formatting is desired 
   * for each existing directory and format them.
   * 
   * @param conf
   * @param isConfirmationNeeded
   * @return true if formatting was aborted, false otherwise
   * @throws IOException
   */
  public static boolean format(Configuration conf,
                                boolean isConfirmationNeeded
                                ) throws IOException {
    
    StateManager stateManager = null;

    try {
      stateManager = new StateManager(conf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
    LOG.info("NAMENODE FORMAT: format namespace and blocksmap");
    try {
      stateManager.format();
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
    LOG.info("NAMENODE FORMAT: format zookeeper");
    cleanupOnZooKeeper(conf);
    
    Collection<File> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    for(Iterator<File> it = dirsToFormat.iterator(); it.hasNext();) {
      File curDir = it.next();
      if (!curDir.exists())
        continue;
      if (isConfirmationNeeded) {
        System.err.print("Re-format filesystem in " + curDir +" ? (Y or N) ");
        if (!(System.in.read() == 'Y')) {
          System.err.println("Format aborted in "+ curDir);
          return true;
        }
        while(System.in.read() != '\n'); // discard the enter-key
      }
    }

    return false;
  }

  private static boolean finalize(Configuration conf,
                               boolean isConfirmationNeeded
                               ) throws IOException {
    /*Collection<File> dirsToFormat = FSNamesystem.getNamespaceDirs(conf);
    Collection<File> editDirsToFormat = 
                               FSNamesystem.getNamespaceEditsDirs(conf);
    FSNamesystem nsys = new FSNamesystem(new FSImage(dirsToFormat,
                                         editDirsToFormat), conf);
    System.err.print(
        "\"finalize\" will remove the previous state of the files system.\n"
        + "Recent upgrade will become permanent.\n"
        + "Rollback option will not be available anymore.\n");
    if (isConfirmationNeeded) {
      System.err.print("Finalize filesystem state ? (Y or N) ");
      if (!(System.in.read() == 'Y')) {
        System.err.println("Finalize aborted.");
        return true;
      }
      while(System.in.read() != '\n'); // discard the enter-key
    }
    nsys.dir.fsImage.finalizeUpgrade();
    return false;*/
    // TODO 
    return true;
  }
  
  @Override
  public void refreshServiceAcl() throws IOException {
    if (!serviceAuthEnabled) {
      throw new AuthorizationException("Service Level Authorization not enabled!");
    }

    SecurityUtil.getPolicy().refresh();
  }
  
  // used to clean the innodb tables in testcases
  public  void cleanNamespaceAndBlocksmap() throws IOException {
    namesystem.cleanNamespaceAndBlocksmap();
  }

  private static void printUsage() {
    System.err.println(
      "Usage: java NameNode [" +
      StartupOption.FORMAT.getName() + "] | [" +
      StartupOption.UPGRADE.getName() + "] | [" +
      StartupOption.ROLLBACK.getName() + "] | [" +
      StartupOption.FINALIZE.getName() + "] | [" +
      StartupOption.IMPORT.getName() + "]");
  }

  private static StartupOption parseArguments(String args[]) {
    int argsLen = (args == null) ? 0 : args.length;
    StartupOption startOpt = StartupOption.REGULAR;
    for(int i=0; i < argsLen; i++) {
      String cmd = args[i];
      if (StartupOption.FORMAT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FORMAT;
      } else if (StartupOption.REGULAR.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.REGULAR;
      } else if (StartupOption.UPGRADE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.UPGRADE;
      } else if (StartupOption.ROLLBACK.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.ROLLBACK;
      } else if (StartupOption.FINALIZE.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.FINALIZE;
      } else if (StartupOption.IMPORT.getName().equalsIgnoreCase(cmd)) {
        startOpt = StartupOption.IMPORT;
      } else
        return null;
    }
    return startOpt;
  }

  private static void setStartupOption(Configuration conf, StartupOption opt) {
    conf.set("dfs.namenode.startup", opt.toString());
  }

  static StartupOption getStartupOption(Configuration conf) {
    return StartupOption.valueOf(conf.get("dfs.namenode.startup",
                                          StartupOption.REGULAR.toString()));
  }

  public static NameNode createNameNode(String argv[], 
                                 Configuration conf) throws IOException {
    if (conf == null)
      conf = new Configuration();
    StartupOption startOpt = parseArguments(argv);
    if (startOpt == null) {
      printUsage();
      return null;
    }
    setStartupOption(conf, startOpt);

    boolean aborted;
    switch (startOpt) {
      case FORMAT:
        aborted = format(conf, true);
        System.exit(aborted ? 1 : 0);
      case FINALIZE:
        aborted = finalize(conf, true);
        System.exit(aborted ? 1 : 0);
      default:
    }

    NameNode namenode = new NameNode(conf);
    return namenode;
  }
    
  public static void main(String argv[]) throws Exception {
    try {
      StringUtils.startupShutdownMessage(NameNode.class, argv, LOG);
      NameNode namenode = createNameNode(argv, null);
      if (namenode != null)
        namenode.join();
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }
}
