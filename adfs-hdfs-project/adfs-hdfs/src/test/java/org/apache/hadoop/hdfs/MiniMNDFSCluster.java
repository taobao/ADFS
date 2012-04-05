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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FSDatasetInterface;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.StaticMapping;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ToolRunner;

public class MiniMNDFSCluster {

//  static {
    /*try {
      HsqldbBlockmap.dropTablesIfExistsAndCreateBy(new Configuration(false));
      System.setProperty("hsclient.simulator.database.description",
                         "nn_state.file_t:id|parent_id|name|length|block_size|replication|atime|mtime|owner:PRIMARY=0|PID_NAME=1,2;");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }*/
//  }
  
  public class DataNodeProperties {
    DataNode datanode;
    Configuration conf;
    String[] dnArgs;

    DataNodeProperties(DataNode node, Configuration conf, String[] args) {
      datanode = node;
      this.conf = conf;
      dnArgs = args;
    }
  }
  
  public class NameNodeProperties {
    NameNode namenode;
    Configuration conf;
    String[] nnArgs;

    NameNodeProperties(NameNode node, Configuration conf, String[] args) {
      namenode = node;
      this.conf = conf;
      nnArgs = args;
    }
  }
  public static Random r = new Random();

  private Configuration namenodeconf;
  private int numNameNodes;
  private final ArrayList<NameNodeProperties> nameNodes = new ArrayList<NameNodeProperties>();
  
  private Configuration datanodeconf;
  private int numDataNodes;
  private final ArrayList<DataNodeProperties> dataNodes = new ArrayList<DataNodeProperties>();
  
  private File dataDir;
  
  private int numDatanodesFootprint = 0;
  private int NnIndex=-1;
  
  /**
   * This null constructor is used only when wishing to start a data node
   * cluster without a name node (ie when the name node is started elsewhere).
   */
  public MiniMNDFSCluster() {}

  /**
   * Modify the config and start up the servers. The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be managed
   * by this class.
   * 
   * @param conf the base configuration to use in starting the servers. This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  public MiniMNDFSCluster(Configuration conf, int numDataNodes, boolean format, String[] racks) throws IOException {
    this(0, conf, numDataNodes, format, true, true, null, racks, null, null);
  }

  public MiniMNDFSCluster(Configuration conf,
                          int numDataNodes,
                          boolean format,
                          String[] racks,
                          boolean dropDB) throws IOException {
    this(0, conf, numDataNodes, format, true, true, null, racks, null, null, dropDB);
  }

  /**
   * Modify the config and start up the servers. The rpc and info ports for
   * servers are guaranteed to use free ports.
   * <p>
   * NameNode and DataNode directory creation and configuration will be managed
   * by this class.
   * 
   * @param conf the base configuration to use in starting the servers. This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostname for each DataNode
   */
  public MiniMNDFSCluster(Configuration conf,
                          int numDataNodes,
                          boolean format,
                          String[] racks,
                          String[] hosts) throws IOException {
    this(0, conf, numDataNodes, format, true, true, null, racks, hosts, null);
  }

  /**
   * Modify the config and start up the servers with the given operation.
   * Servers will be started on free ports.
   * <p>
   * The caller must manage the creation of NameNode and DataNode directories
   * and have already set dfs.name.dir and dfs.data.dir in the given conf.
   * 
   * @param conf the base configuration to use in starting the servers. This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param nameNodeOperation the operation with which to start the servers. If
   *          null or StartupOption.FORMAT, then StartupOption.REGULAR will be
   *          used.
   */
  public MiniMNDFSCluster(Configuration conf, int numDataNodes, StartupOption nameNodeOperation) throws IOException {
    this(0, conf, numDataNodes, false, false, false, nameNodeOperation, null, null, null);
  }

  /**
   * NOTE: if possible, the other constructors that don't have nameNode port
   * parameter should be used as they will ensure that the servers use free
   * ports.
   * <p>
   * Modify the config and start up the servers.
   * 
   * @param nameNodePort suggestion for which rpc port to use. caller should use
   *          getNameNodePort() to get the actual port used.
   * @param conf the base configuration to use in starting the servers. This
   *          will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param format if true, format the NameNode and DataNodes before starting up
   * @param manageDfsDirs if true, the data directories for servers will be
   *          created and dfs.name.dir and dfs.data.dir will be set in the conf
   * @param operation the operation with which to start the servers. If null or
   *          StartupOption.FORMAT, then StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   */
  public MiniMNDFSCluster(int nameNodePort,
                          Configuration conf,
                          int numDataNodes,
                          boolean format,
                          boolean manageDfsDirs,
                          StartupOption operation,
                          String[] racks) throws IOException {
    this(nameNodePort,
         conf,
         numDataNodes,
         format,
         manageDfsDirs,
         manageDfsDirs,
         operation,
         racks,
         null,
         null);
  }
  

  public MiniMNDFSCluster(int nameNodePort,
                          Configuration conf,
                          int numDataNodes,
                          boolean format,
                          boolean manageNameDfsDirs,
                          boolean manageDataDfsDirs,
                          StartupOption operation,
                          String[] racks,
                          String hosts[],
                          long[] simulatedCapacities) throws IOException {
    this(conf,
         numDataNodes,
         format,
         manageNameDfsDirs,
         manageDataDfsDirs,
         operation,
         racks,
         hosts,
         simulatedCapacities,false);
  }

  // this constructor need to change - refer to the above constructor
  public MiniMNDFSCluster(int nameNodePort,
                          Configuration conf,
                          int numDataNodes,
                          boolean format,
                          boolean manageNameDfsDirs,
                          boolean manageDataDfsDirs,
                          StartupOption operation,
                          String[] racks,
                          String hosts[],
                          long[] simulatedCapacities,
                          boolean dropDB) throws IOException {
    this(conf,
         numDataNodes,
         format,
         manageNameDfsDirs,
         manageDataDfsDirs,
         operation,
         racks,
         hosts,
         simulatedCapacities, dropDB);
  }
  
  /**
   * for choosing the specified namenode to start all datanode
   * @param conf
   * @param numDataNodes
   * @param nameNodeNumber
   * @param format
   * @param racks
   * @throws IOException
   */
  public MiniMNDFSCluster(Configuration conf, int numDataNodes, int NnIndex,boolean format, String[] racks) throws IOException {
    this(conf, numDataNodes, NnIndex , format, true, true, null, racks, null, null);
  }  


  public MiniMNDFSCluster(Configuration conf,
                          int numDataNodes,
                          boolean format,
                          boolean manageNameDfsDirs,
                          boolean manageDataDfsDirs,
                          StartupOption operation,
                          String[] racks,
                          String hosts[],
                          long[] simulatedCapacities,
                          boolean dropDB) throws IOException {
    this.namenodeconf = conf;
    
    this.loadMiniDfsConf();

    ArrayList<Integer> nameNodePorts = new ArrayList<Integer>();
    String portstr = this.namenodeconf.get("dfs.namenode.port.list", "0");
    String[] ports = portstr.split(",");
    for (int i = 0; i < ports.length; i++) {
      nameNodePorts.add(Integer.parseInt(ports[i]));
    }

    NnIndex=-1;
    this.numNameNodes = nameNodePorts.size();

    String recheck = conf.get("heartbeat.recheck.interval");
    if(recheck == null || Integer.parseInt(recheck) < 5000) {
      conf.setInt("heartbeat.recheck.interval", 5000);
    }
    namenodeconf.setInt("dfs.heartbeat.interval", 2);
    String dnmap = conf.get("datanodemap.update.interval");
    if(dnmap == null || Integer.parseInt(dnmap) > 3000) {
      conf.setInt("datanodemap.update.interval", 3000);
    }
    namenodeconf.setInt("zookeeper.session.timeout", 180000);
    namenodeconf.setInt("dfs.replication.pending.timeout.sec", 2);
    namenodeconf.setBoolean("test.close.zookeeper", false);

    if (dropDB) {
      try {
        this.cleanNSandBM();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    dataDir = new File(getBaseDir(), "data");

    setCurrentUserBy(namenodeconf);
    setNameDirIfNeed(manageNameDfsDirs);
    setDfsReplication(numDataNodes);
    setNodeSwitchMappingImpl(DNSToSwitchMapping.class, StaticMapping.class);
    formatNameIfNeed(format);

    for (int i = 0; i < numNameNodes; i++) {
      setDefaultUrl("hdfs://localhost:" + nameNodePorts.get(i));
      conf.set("dfs.http.address", "127.0.0.1:0");
      NameNode nameNode = NameNode.createNameNode(args(operation), this.namenodeconf);
      this.nameNodes.add(new NameNodeProperties(nameNode,
                                                new Configuration(this.namenodeconf),
                                                null));
    }
    startDataNodes(conf,
                   numDataNodes,
                   manageDataDfsDirs,
                   operation,
                   racks,
                   hosts,
                   simulatedCapacities);
    waitClusterUp();
  }

  
  public MiniMNDFSCluster(Configuration conf,
                          int numDataNodes,
                          int NnIndex,
                          boolean format,
                          boolean manageNameDfsDirs,
                          boolean manageDataDfsDirs,
                          StartupOption operation,
                          String[] racks,
                          String hosts[],
                          long[] simulatedCapacities) throws IOException {
    
    String recheck = conf.get("heartbeat.recheck.interval");
    if(recheck == null || Integer.parseInt(recheck) < 5000) {
      conf.setInt("heartbeat.recheck.interval", 5000);
    }
    conf.setInt("dfs.heartbeat.interval", 2);
    String dnmap = conf.get("datanodemap.update.interval");
    if(dnmap == null || Integer.parseInt(dnmap) > 3000) {
      conf.setInt("datanodemap.update.interval", 3000);
    }
    conf.setInt("dfs.replication.pending.timeout.sec", 2);
    conf.setInt("zookeeper.session.timeout", 180000);
    conf.setBoolean("test.close.zookeeper", false);
    
    this.namenodeconf = conf;
    this.loadMiniDfsConf();
    
    ArrayList<Integer> nameNodePorts = new ArrayList<Integer>();
    String portstr = this.namenodeconf.get("dfs.namenode.port.list", "0");
    String[] ports = portstr.split(",");
    if(NnIndex<0 || NnIndex>ports.length-1 || ports.length==1){
      throw new IOException("select namenode number wrong,check the parameter");
    }
    this.NnIndex=NnIndex;
    
    for (int i = 0; i < ports.length; i++) {
      nameNodePorts.add(Integer.parseInt(ports[i]));
    }

    this.numNameNodes = nameNodePorts.size();

    dataDir = new File(getBaseDir(), "data");

    setCurrentUserBy(namenodeconf);
    setNameDirIfNeed(manageNameDfsDirs);
    setDfsReplication(numDataNodes);
    setNodeSwitchMappingImpl(DNSToSwitchMapping.class, StaticMapping.class);

    this.formatNameIfNeed(format);

    for (int i = 0; i < numNameNodes; i++) {
      setDefaultUrl("hdfs://localhost:" + nameNodePorts.get(i));
      conf.set("dfs.http.address", "127.0.0.1:0");
      NameNode nameNode = NameNode.createNameNode(args(operation), this.namenodeconf);
      this.nameNodes.add(new NameNodeProperties(nameNode,
                                                new Configuration(this.namenodeconf),
                                                null));
    } 
    
    startDataNodes(conf,
                   numDataNodes,
                   manageDataDfsDirs,
                   operation,
                   racks,
                   hosts,
                   simulatedCapacities);
    waitClusterUp();
  }
  
  public synchronized void cleanNSandBM() {
    int index = r.nextInt(this.nameNodes.size());
    try {
      this.nameNodes.get(index).namenode.cleanNamespaceAndBlocksmap();
    } catch( Exception e){
      e.printStackTrace();
    }
  }
  
  private void loadMiniDfsConf() {
    URL url = this.getClass().getResource("/mini-dfs-conf.xml");
    this.namenodeconf.addResource(url);
  }

  private void setCurrentUserBy(Configuration conf) throws IOException {
    UserGroupInformation.setCurrentUser(UserGroupInformation.readFrom(conf));
  }


  private static void tryDelete(File dir) throws IOException {
    if (dir.exists() && !FileUtil.fullyDelete(dir)) { throw new IOException("Cannot remove data directory: " + dir); }
  }

  /**
   * If the NameNode is running, attempt to finalize a previous upgrade. When this method return, the NameNode should be
   * finalized, but DataNodes may not be since that occurs asynchronously.
   * 
   * @throws IllegalStateException if the Namenode is not running.
   */
  //need update
  public void finalizeCluster(Configuration conf) throws Exception {
    if (this.nameNodes == null) { throw new IllegalStateException("Attempting to finalize "
        + "Namenode but it is not running"); }
    ToolRunner.run(new DFSAdmin(conf), new String[] { "-finalizeUpgrade" });
  }

  public void formatDataNodeDirs() throws IOException {
    dataDir = new File(getBaseDir(), "data");
    tryDelete(dataDir);
  }

  /**
   * @return block reports from all data nodes Block[] is indexed in the same order as the list of datanodes returned by
   *         getDataNodes()
   */
  public Block[][] getAllBlockReports() {
    int numDataNodes = dataNodes.size();
    Block[][] result = new Block[numDataNodes][];
    for (int i = 0; i < numDataNodes; ++i) {
      result[i] = getBlockReport(i);
    }
    return result;
  }

  /**
   * @param dataNodeIndex - data node whose block report is desired - the index is same as for getDataNodes()
   * @return the block report for the specified data node
   */
  public Block[] getBlockReport(int dataNodeIndex) {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) { throw new IndexOutOfBoundsException(); }
    return dataNodes.get(dataNodeIndex).datanode.getFSDataset().getBlockReport();
  }

  /**
   * Access to the data directory used for Datanodes
   * 
   * @throws IOException
   */
  public String getDataDirectory() {
    return dataDir.getAbsolutePath();
  }

  /** @return the datanode having the ipc server listen port */
  public DataNode getDataNode(int ipcPort) {
    for (DataNode dn : getDataNodes()) {
      if (dn.ipcServer.getListenerAddress().getPort() == ipcPort) { return dn; }
    }
    return null;
  }

  /**
   * Gets a list of the started DataNodes. May be empty.
   */
  public ArrayList<DataNode> getDataNodes() {
    ArrayList<DataNode> list = new ArrayList<DataNode>();
    for (int i = 0; i < dataNodes.size(); i++) {
      DataNode node = dataNodes.get(i).datanode;
      list.add(node);
    }
    return list;
  }

  /**
   * Get a client handle to the DFS cluster.
   */
  public FileSystem getFileSystem() throws IOException {
    int index = r.nextInt(this.nameNodes.size());
    return FileSystem.get(this.nameNodes.get(index).conf);
  }

  /**
   * allocate namenode which client handle connect to.
   */
  public FileSystem getFileSystem(int index) throws IOException {
    assert index < this.numNameNodes : "index overflow";
    return FileSystem.get(this.nameNodes.get(index).conf);
  }
  
  public InetSocketAddress getFreeNamenode(int exclude) throws IOException {

    for (int i = 0; i < this.numNameNodes; i++) {
      if (getDatanodeReportOfOneNamenode(DatanodeReportType.LIVE, i).length == 0) {
        Configuration conf = this.nameNodes.get(i).conf;
        long datanodeMapUpdateInterval = conf.getInt("datanodemap.update.interval", 5 * 60 * 1000); // 5 minutes
        long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
        long datanodeExpireInterval = 2 * datanodeMapUpdateInterval + 10 * heartbeatInterval;
        try {
          Thread.sleep(datanodeExpireInterval);
        } catch (Exception e) {}
        
        return this.nameNodes.get(i).namenode.getNameNodeAddress();
      }
    }
    return this.nameNodes.get((0 == exclude && this.numNameNodes > 1)? 1 :0).namenode.getNameNodeAddress();
  }
  
  public FileSystem getFileSystemCToFreeNamenode(int exclude) throws IOException {
    for (int i = 0; i < this.numNameNodes; i++) {
      if (getDatanodeReportOfOneNamenode(DatanodeReportType.LIVE, i).length == 0) {
        Configuration conf = this.nameNodes.get(i).conf;
        long datanodeMapUpdateInterval = conf.getInt("datanodemap.update.interval", 5 * 60 * 1000); // 5 minutes
        long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000;
        long datanodeExpireInterval = 2 * datanodeMapUpdateInterval + 10 * heartbeatInterval;
        try {
          Thread.sleep(datanodeExpireInterval);
        } catch (Exception e) {}
        
        return FileSystem.get(this.nameNodes.get(i).conf);
      }
    }
    return FileSystem.get(this.nameNodes.get((0 == exclude && this.numNameNodes > 1)? 1 :0).conf);
  }
  
  /**
   * Get the directories where the namenode stores its image.
   */
  public Collection<File> getNameDirs() {
   /* return FSNamesystem.getNamespaceDirs(conf);*/
    return null;
  }
  
  public Collection<File> getParticularNameDirs(int index) {
    assert index < this.numNameNodes && index >= 0 : "index overflow";
    return FSNamesystem.getNamespaceDirs(nameNodes.get(index).conf);
  }

  /**
   * Get the directories where the namenode stores its edits.
   */
  public Collection<File> getNameEditsDirs() {
    /*return FSNamesystem.getNamespaceEditsDirs(conf);*/
    return null;
  }
  
  public Collection<File> getNameEditsDirs(int index) {
    assert index < this.numNameNodes && index >= 0 : "index overflow";
    return FSNamesystem.getNamespaceEditsDirs(nameNodes.get(index).conf);
  }

  public NameNode getNameNode(int index) {
    assert index < this.numNameNodes && index >= 0 : "index overflow";
    return this.nameNodes.get(index).namenode;
  }
  
  public NameNodeProperties getNameNodeProp(int index) {
    assert index < this.numNameNodes && index >= 0 : "index overflow";
    return this.nameNodes.get(index);
  }

  public ArrayList<NameNode> getNameNodes() {
    ArrayList<NameNode> res = new ArrayList<NameNode>();
    for (int i = 0; i < nameNodes.size(); i++) {
      res.add(nameNodes.get(i).namenode);
    }
    return res;
  }
  
  public NameNode[] listNameNodes() {
    NameNode[] list = new NameNode[nameNodes.size()];
    for (int i = 0; i < nameNodes.size(); i++) {
      list[i] = nameNodes.get(i).namenode;
    }
    return list;
  }
  
  /**
   * Gets the rpc port used by the NameNode, because the caller supplied port is not necessarily the actual port used.
   */
  public int getNameNodePort(int index) {
    assert index < this.numNameNodes && index >= 0 : "index overflow";
    return nameNodes.get(index).namenode.getNameNodeAddress().getPort();
  }
  
  public ArrayList<Integer> getNameNodePortList() {
    ArrayList<Integer> res = new ArrayList<Integer>();
    for (int i = 0; i < nameNodes.size(); i++) {
      res.add(nameNodes.get(i).namenode.getNameNodeAddress().getPort());
    }
    return res;
  }
  
  // get one port of namenode randomly
  public int getNameNodePort() {
    Random r = new Random();
    int index = r.nextInt(this.nameNodes.size());
    return nameNodes.get(index).namenode.getNameNodeAddress().getPort();
  }

  /**
   * This method is valid only if the data nodes have simulated data
   * 
   * @param blocksToInject - blocksToInject[] is indexed in the same order as the list of datanodes returned by
   *          getDataNodes()
   * @throws IOException if not simulatedFSDataset if any of blocks already exist in the data nodes Note the rest of the
   *           blocks are not injected.
   */
  public void injectBlocks(Block[][] blocksToInject) throws IOException {
    if (blocksToInject.length > dataNodes.size()) { throw new IndexOutOfBoundsException(); }
    for (int i = 0; i < blocksToInject.length; ++i) {
      injectBlocks(i, blocksToInject[i]);
    }
  }

  /**
   * This method is valid only if the data nodes have simulated data
   * 
   * @param dataNodeIndex - data node i which to inject - the index is same as for getDataNodes()
   * @param blocksToInject - the blocks
   * @throws IOException if not simulatedFSDataset if any of blocks already exist in the data node
   * 
   */
  public void injectBlocks(int dataNodeIndex, Block[] blocksToInject) throws IOException {
    if (dataNodeIndex < 0 || dataNodeIndex > dataNodes.size()) { throw new IndexOutOfBoundsException(); }
    FSDatasetInterface dataSet = dataNodes.get(dataNodeIndex).datanode.getFSDataset();
    if (!(dataSet instanceof SimulatedFSDataset)) { throw new IOException("injectBlocks is valid only for SimilatedFSDataset"); }
    SimulatedFSDataset sdataset = (SimulatedFSDataset) dataSet;
    sdataset.injectBlocks(blocksToInject);
    dataNodes.get(dataNodeIndex).datanode.scheduleBlockReport(0);
  }

  /**
   * Returns true if the NameNode is running and is out of Safe Mode.
   */
  // need update
  public boolean isClusterUp() {
    if (this.nameNodes.size() == 0) { return false; }
/*  long[] sizes = nameNode.getStats();
    boolean isUp = false;
    synchronized (this) {
      isUp = (!nameNode.isInSafeMode() && sizes[0] != 0);
    }
    return isUp;*/
    return true;
  }

  /**
   * Returns true if there is at least one DataNode running.
   */
  public boolean isDataNodeUp() {
    if (dataNodes == null || dataNodes.size() == 0) { return false; }
    return true;
  }

  /**
   * Restart a datanode
   * 
   * @param dnprop datanode's property
   * @return true if restarting is successful
   * @throws IOException
   */
  public synchronized boolean restartDataNode(DataNodeProperties dnprop) throws IOException {
    Configuration conf = dnprop.conf;
    String[] args = dnprop.dnArgs;
    Configuration newconf = new Configuration(conf); // save cloned config
    dataNodes.add(new DataNodeProperties(DataNode.createDataNode(args, conf), newconf, args));
    numDataNodes++;
    return true;
  }

  /*
   * Restart a particular datanode
   */
  public synchronized boolean restartDataNode(int i) throws IOException {
    DataNodeProperties dnprop = stopDataNode(i);
    if (dnprop == null) {
      return false;
    } else {
      return restartDataNode(dnprop);
    }
  }
  
  /*
   * Restart a particular datanode with new configuration
   */
  public synchronized boolean restartDataNode(int i, Configuration conf) throws IOException { 
    DataNodeProperties dnprop = stopDataNode(i);
    if ( dnprop == null ) {
      return false;
    } else {
      dnprop.conf = conf;
      return restartDataNode(dnprop);
    }
  }
  /*
   * Restart all datanodes
   */
  public synchronized boolean restartDataNodes() throws IOException {
    for (int i = dataNodes.size() - 1; i >= 0; i--) {
      System.out.println("Restarting DataNode " + i);
      if (!restartDataNode(i)) return false;
    }
    return true;
  }
  
  /**
   * Restart namenode
   * @throws IOException 
   */
  public synchronized void restartNamenode() throws IOException {
    restartNamenode(0);
  }
  
  public synchronized void restartNamenode(int nnIndex) throws IOException {
    if(nnIndex >nameNodes.size())
      throw new IllegalArgumentException("Namenode index is beyond boundary");
    NameNodeProperties nnPro=nameNodes.get(nnIndex);
    Configuration conf = nnPro.conf;
    shutdownNamenode(nnIndex);
    NameNode nn = NameNode.createNameNode(new String[] {}, conf);
    NameNodeProperties nnprop = new NameNodeProperties(nn,
        new Configuration(conf),
        null);
    nameNodes.set(nnIndex, nnprop);
    waitClusterUp();
    waitActive();
  }
  
  
  /**
   * Shutdown the give namenode
   */
  public synchronized void shutdownNamenode(int nnIndex){
    if(nnIndex >nameNodes.size())
      throw new IllegalArgumentException("Namenode index is beyond boundary");
    NameNode nn=nameNodes.get(nnIndex).namenode;
    if(nn!=null){
      nn.stop();
      nn.join();
      Configuration conf = nameNodes.get(nnIndex).conf;
      NameNodeProperties nnprop = new NameNodeProperties(null,
          new Configuration(conf),
          null);
      nameNodes.set(nnIndex, nnprop);
    }
  }
  
  /**
   * Shut down the servers that are up.
   */
  public void shutdown(boolean dropDB){
    System.out.println("Shutting down the Mini HDFS Cluster");
    shutdownDataNodes();
    if(dropDB) {
      cleanNSandBM(); 
    }
    shutdownNameNodes();
    
    try {
      File namespaceForTest = new File(namenodeconf.get("distributed.data.path"));
      FileUtil.fullyDelete(namespaceForTest);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * Shut down the servers that are up.
   */
  public void shutdown(){
    System.out.println("Shutting down the Mini HDFS Cluster");
    shutdownDataNodes();
    cleanNSandBM(); 
    shutdownNameNodes();
    
    try {
      File namespaceForTest = new File(namenodeconf.get("distributed.data.path"));
      FileUtil.fullyDelete(namespaceForTest);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  /**
   * Shutdown all namenodes 
   */
  public void shutdownNameNodes() {
    for (int i = nameNodes.size() - 1; i >= 0; i--) {
      System.out.println("Shutting down NameNode " + i);
      NameNode namenode = nameNodes.remove(i).namenode;
      if (namenode != null) {
        namenode.stop();
        namenode.join();
        namenode = null;
      }
      numNameNodes--;
    }
  }
  
  /**
   * Shutdown all DataNodes started by this class. The NameNode is left running so that new DataNodes may be started.
   */
  public void shutdownDataNodes() {
    for (int i = dataNodes.size() - 1; i >= 0; i--) {
      System.out.println("Shutting down DataNode " + i);
      DataNode dn = dataNodes.remove(i).datanode;
      dn.shutdown();
      numDataNodes--;
    }
  }

  /**
   * Modify the config and start up the DataNodes. The info port for DataNodes is guaranteed to use a free port.
   * 
   * @param conf the base configuration to use in starting the DataNodes. This will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be created and dfs.data.dir will be set in
   *          the conf
   * @param operation the operation with which to start the DataNodes. If null or StartupOption.FORMAT, then
   *          StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * 
   * @throws IllegalStateException if NameNode has been shutdown
   */

  public void startDataNodes(Configuration conf,
                             int numDataNodes,
                             boolean manageDfsDirs,
                             StartupOption operation,
                             String[] racks) throws IOException {
    this.NnIndex=-1;
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null, null);
  }

  /**
   * Modify the config and start up additional DataNodes. The info port for DataNodes is guaranteed to use a free port.
   * 
   * Data nodes can run with the name node in the mini cluster or a real name node. For example, running with a real
   * name node is useful when running simulated data nodes with a real name node. If minicluster's name node is null
   * assume that the conf has been set with the right address:port of the name node.
   * 
   * @param conf the base configuration to use in starting the DataNodes. This will be modified as necessary.
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be created and dfs.data.dir will be set in
   *          the conf
   * @param operation the operation with which to start the DataNodes. If null or StartupOption.FORMAT, then
   *          StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param simulatedCapacities array of capacities of the simulated data nodes
   * 
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public void startDataNodes(Configuration conf,
                             int numDataNodes,
                             boolean manageDfsDirs,
                             StartupOption operation,
                             String[] racks,
                             long[] simulatedCapacities) throws IOException {
    this.NnIndex=-1;
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null, simulatedCapacities);

  }
  
  /**
   * start datanodes connect to the particular namenode
   * @param conf 
   * @param numDataNodes  the number of Datanodes to start
   * @param NnIndex  the namenode index which the datanodes to connect
   * @param manageDfsDirs
   * @param operation
   * @param racks
   * @param simulatedCapacities
   * @throws IOException
   */
  public void startDataNodes(Configuration conf,
                             int numDataNodes,
                             int NnIndex,
                             boolean manageDfsDirs,
                             StartupOption operation,
                             String[] racks,
                             long[] simulatedCapacities) throws IOException {
    String portstr = this.namenodeconf.get("dfs.namenode.port.list", "0");
    String[] ports = portstr.split(",");
    if(NnIndex<0 || NnIndex>ports.length){
      throw new IOException("select namenode number wrong,check the parameter");
    }
    this.NnIndex=NnIndex;
    startDataNodes(conf, numDataNodes, manageDfsDirs, operation, racks, null, simulatedCapacities);

  }

  /**
   * Modify the config and start up additional DataNodes. The info port for DataNodes is guaranteed to use a free port.
   * 
   * Data nodes can run with the name node in the mini cluster or a real name node. For example, running with a real
   * name node is useful when running simulated data nodes with a real name node. If minicluster's name node is null
   * assume that the conf has been set with the right address:port of the name node.
   * 
   * @param conf
   * 
   * @param numDataNodes Number of DataNodes to start; may be zero
   * @param manageDfsDirs if true, the data directories for DataNodes will be created and dfs.data.dir will be set in
   *          the conf
   * @param operation the operation with which to start the DataNodes. If null or StartupOption.FORMAT, then
   *          StartupOption.REGULAR will be used.
   * @param racks array of strings indicating the rack that each DataNode is on
   * @param hosts array of strings indicating the hostnames for each DataNode
   * @param simulatedCapacities array of capacities of the simulated data nodes
   * 
   * @throws IllegalStateException if NameNode has been shutdown
   */
  public synchronized void startDataNodes(Configuration conf, int numDataNodes,
                                          boolean manageDfsDirs,
                                          StartupOption operation,
                                          String[] racks,
                                          String[] hosts,
                                          long[] simulatedCapacities) throws IOException {

    int curDatanodesNum = dataNodes.size();
    // If minicluster's name node is null assume that the conf has been
    // set with the right address:port of the name node.
    //
    assert nameNodes.size() == this.numNameNodes : "numNameNodes must equal to nameNodes size";
    String namenodesStr = "";
    if ( nameNodes.size() > 0) {
      if(NnIndex==-1){
        for (int i = 0; i < nameNodes.size(); i++) {
          InetSocketAddress nnAddr = nameNodes.get(i).namenode.getNameNodeAddress();
          int nameNodePort = nnAddr.getPort();
          namenodesStr = namenodesStr + "hdfs://" + nnAddr.getHostName() + ":" + Integer.toString(nameNodePort);
          if (i != nameNodes.size() - 1) {
            namenodesStr = namenodesStr + ",";
          }
        }
      }else{
        InetSocketAddress nnAddr = nameNodes.get(NnIndex).namenode.getNameNodeAddress();
        int nameNodePort=nnAddr.getPort();
        namenodesStr=namenodesStr+"hdfs://"+nnAddr.getHostName()+":"+Integer.toString(nameNodePort);
      }
    }
    
    datanodeconf = conf;
    datanodeconf.set("dfs.namenode.rpcaddr.list", namenodesStr);
    
    if (racks != null && numDataNodes > racks.length) { throw new IllegalArgumentException("The length of racks ["
        + racks.length + "] is less than the number of datanodes [" + numDataNodes + "]."); }
    if (hosts != null && numDataNodes > hosts.length) { throw new IllegalArgumentException("The length of hosts ["
        + hosts.length + "] is less than the number of datanodes [" + numDataNodes + "]."); }
    // Generate some hostnames if required
	  System.out.println("Generating host names for datanodes");
	  hosts = new String[numDataNodes];
	  for (int i = curDatanodesNum; i < curDatanodesNum + numDataNodes; i++) {
	    hosts[i - curDatanodesNum] = "192.168.0." + i ;
	  }

    if (simulatedCapacities != null && numDataNodes > simulatedCapacities.length) { throw new IllegalArgumentException("The length of simulatedCapacities ["
        + simulatedCapacities.length + "] is less than the number of datanodes [" + numDataNodes + "]."); }

    String[] dnArgs =
      (operation == null || operation != StartupOption.ROLLBACK) ? null : new String[] { operation.getName() };

    for (int i = curDatanodesNum; i < curDatanodesNum + numDataNodes; i++) {
      Configuration dnConf = new Configuration(conf);
      if (manageDfsDirs) {
        File dir1 = new File(dataDir, "data" + (2 * numDatanodesFootprint + 1));
        File dir2 = new File(dataDir, "data" + (2 * numDatanodesFootprint + 2));
        dir1.mkdirs();
        dir2.mkdirs();
        if (!dir1.isDirectory() || !dir2.isDirectory()) { throw new IOException("Mkdirs failed to create directory for DataNode "
            + i + ": " + dir1 + " or " + dir2); }
        dnConf.set("dfs.data.dir", dir1.getPath() + "," + dir2.getPath());
        
        numDatanodesFootprint = numDatanodesFootprint + 1;
      }
      if (simulatedCapacities != null) {
        dnConf.setBoolean("dfs.datanode.simulateddatastorage", true);
        dnConf.setLong(SimulatedFSDataset.CONFIG_PROPERTY_CAPACITY, simulatedCapacities[i - curDatanodesNum]);
      }
      System.out.println("Starting DataNode " + i + " with dfs.data.dir: " + dnConf.get("dfs.data.dir"));
      if (hosts != null) {
        dnConf.set("slave.host.name", hosts[i - curDatanodesNum]);
        System.out.println("Starting DataNode " + i + " with hostname set to: " + dnConf.get("slave.host.name"));
      }
      if (racks != null) {
        String name = hosts[i - curDatanodesNum];
        System.out.println("Adding node with hostname : " + name + " to rack " + racks[i - curDatanodesNum]);
        StaticMapping.addNodeToRack(name, racks[i - curDatanodesNum]);
      }
      Configuration newconf = new Configuration(dnConf); // save config
      if (hosts != null) {
        NetUtils.addStaticResolution(hosts[i - curDatanodesNum], "localhost");
//    	  NetUtils.addStaticResolution(hosts[i - curDatanodesNum], hosts[i-curDatanodesNum]);
      }
      DataNode dn = DataNode.instantiateDataNode(dnArgs, dnConf);
      // since the HDFS does things based on IP:port, we need to add the mapping
      // for IP:port to rackId
      String ipAddr = dn.getSelfAddr().getAddress().getHostAddress();
      if (racks != null) {
        int port = dn.getSelfAddr().getPort();
        System.out.println("Adding node with IP:port : " + ipAddr + ":" + port + " to rack "
            + racks[i - curDatanodesNum]);
        StaticMapping.addNodeToRack(ipAddr + ":" + port, racks[i - curDatanodesNum]);
      }
      DataNode.runDatanodeDaemon(dn,true);
      dataNodes.add(new DataNodeProperties(dn, newconf, dnArgs));
    }
    curDatanodesNum += numDataNodes;
    this.numDataNodes += numDataNodes;
    waitActive();
  }

  /*
   * Shutdown a particular datanode
   */
  public DataNodeProperties stopDataNode(int i) {
    if (i < 0 || i >= dataNodes.size()) { return null; }
    DataNodeProperties dnprop = dataNodes.remove(i);
    DataNode dn = dnprop.datanode;
    System.out.println("MiniDFSCluster Stopping DataNode " + dn.dnRegistration.getName() + " from a total of "
        + (dataNodes.size() + 1) + " datanodes.");
    dn.shutdown();
    numDataNodes--;
    return dnprop;
  }

  /*
   * Shutdown a datanode by name.
   */
  public synchronized DataNodeProperties stopDataNode(String name) {
    int i;
    for (i = 0; i < dataNodes.size(); i++) {
      DataNode dn = dataNodes.get(i).datanode;
      if (dn.dnRegistration.getName().equals(name)) {
        break;
      }
    }
    return stopDataNode(i);
  }

  public NameNodeProperties startNewNameNode(Configuration conf, int port, StartupOption operation) throws IOException {
    setDefaultUrl("hdfs://localhost:" + port);
    NameNode nameNode = NameNode.createNameNode(args(operation), conf);
    NameNodeProperties nnprop = new NameNodeProperties(nameNode,
                           new Configuration(conf),
                           null);
    this.nameNodes.add(nnprop);
    this.numNameNodes++;
    return nnprop;
    
  }
  
  public boolean startNewNameNode(NameNodeProperties nnprop, StartupOption operation) throws IOException {
    NameNode nameNode = NameNode.createNameNode(args(operation), nnprop.conf);
    nnprop.namenode = nameNode;
    this.nameNodes.add(nnprop);
    this.numNameNodes++;
    return true; 
  }
  
  /*
   * Shutdown a particular namenode
   */
  public NameNodeProperties stopNameNode(int i) {
    if (i < 0 || i >= nameNodes.size()) { return null; }
    NameNodeProperties nnprop = nameNodes.remove(i);
    NameNode nn = nnprop.namenode;
    System.out.println("MiniDFSCluster Stopping NameNode " + nn.getNameNodeAddress().getHostName() + " from a total of "
        + (nameNodes.size() + 1) + " namenodes.");

    nn.stop();
    nn.join();
    nn = null;
    
    numNameNodes--;
    return nnprop;
  }

  /*
   * Shutdown a namenode by name.
   */
  public synchronized NameNodeProperties stopNameNode(String name) {
    int i;
    for (i = 0; i < nameNodes.size(); i++) {
      NameNode nn = nameNodes.get(i).namenode;
      if (nn.getNameNodeAddress().getHostName().equals(name)) {
        break;
      }
    }
    return stopNameNode(i);
  }
  
  /**
   * Get all Datanodes report with particular report-type
   */
  public DatanodeInfo[] getDataNodeReportType(DatanodeReportType type) throws IOException {
    ArrayList<DatanodeInfo> datanodeinfolist = new ArrayList<DatanodeInfo>();
    for (int i = 0; i < this.nameNodes.size(); i++) {
      InetSocketAddress addr = new InetSocketAddress("localhost", getNameNodePort(i));
      DFSClient client = new DFSClient(addr, this.nameNodes.get(i).conf);
      DatanodeInfo[] datanodeinfos = client.datanodeReport(type);
      for (int j = 0; j < datanodeinfos.length; j++) {
        if (!datanodeinfolist.contains(datanodeinfos[j])) {
          datanodeinfolist.add(datanodeinfos[j]);
        }
      }
      client.close();
    }
    DatanodeInfo[] res = new DatanodeInfo[datanodeinfolist.size()];
    for (int i = 0; i < datanodeinfolist.size(); i++) {
      res[i] = (DatanodeInfo)(datanodeinfolist.get(i));
    }
    return res;
  }
  
  /**
   * Get all Datanodes report of one Namenode with particular report-type
   */
  public DatanodeInfo[] getDatanodeReportOfOneNamenode(DatanodeReportType type, int index) throws IOException {
    assert index < this.numNameNodes && index >= 0 : "index overflow";
    InetSocketAddress addr = new InetSocketAddress("localhost", getNameNodePort(index));
    DFSClient client = new DFSClient(addr, this.nameNodes.get(index).conf);
    DatanodeInfo[] datanodeinfos = client.datanodeReport(type);
    return datanodeinfos;
  }
  
  /**
   * Wait until the cluster is active and running.
   */
  public void waitActive() throws IOException {
    waitActive(true);
  }

  
  /**
   * Wait until the cluster is active.
   * 
   * @param waitHeartbeats if true, will wait until all DNs have heartbeat
   */
  public void waitActive(boolean waitHeartbeats) throws IOException {
    if (this.nameNodes.size() == 0) { return; }
    DatanodeInfo[] dnis = getDataNodeReportType(DatanodeReportType.LIVE);
    // make sure all datanodes are alive and sent heartbeat
    while (shouldWait(dnis, waitHeartbeats)) {
      dnis = getDataNodeReportType(DatanodeReportType.LIVE);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
  }

  /**
   * wait for the cluster to get out of safemode.
   */
  public void waitClusterUp() {
    if (numDataNodes > 0) {
      while (!isClusterUp()) {
        try {
          System.err.println("Waiting for the Mini HDFS Cluster to start...");
          Thread.sleep(1000);
        } catch (InterruptedException e) {}
      }
    }
  }

  /**
   * Wait for the given datanode to heartbeat once.
   */
  public void waitForDNHeartbeat(int dnIndex, long timeoutMillis) throws IOException, InterruptedException {
    DataNode dn = getDataNodes().get(dnIndex);

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() < startTime + timeoutMillis) {
      for (int index = 0; index  < this.nameNodes.size(); index++) {
        InetSocketAddress addr = new InetSocketAddress("localhost", getNameNodePort(index));
        DFSClient client = new DFSClient(addr, this.nameNodes.get(index).conf);
        DatanodeInfo report[] = client.datanodeReport(DatanodeReportType.LIVE);
        for (DatanodeInfo thisReport : report) {
          if (thisReport.getStorageID().equals(dn.dnRegistration.getStorageID())) {
            if (thisReport.getLastUpdate() > startTime) return;
          }
        }
      }
      Thread.sleep(500);
    }
  }

  /*
   * Corrupt a block on a particular datanode
   */
  boolean corruptBlockOnDataNode(int i, String blockName) throws Exception {
    Random random = new Random();
    boolean corrupted = false;
    File dataDir = new File(System.getProperty("test.build.data", "build/test/data"), "dfs/data");
    if (i < 0 || i >= dataNodes.size()) return false;
    for (int dn = i * 2; dn < i * 2 + 2; dn++) {
      File blockFile = new File(dataDir, "data" + (dn + 1) + "/current/" + blockName);
      System.out.println("Corrupting for: " + blockFile);
      if (blockFile.exists()) {
        // Corrupt replica by writing random bytes into replica
        RandomAccessFile raFile = new RandomAccessFile(blockFile, "rw");
        FileChannel channel = raFile.getChannel();
        String badString = "BADBAD";
        int rand = random.nextInt((int) channel.size() / 2);
        raFile.seek(rand);
        raFile.write(badString.getBytes());
        raFile.close();
      }
      corrupted = true;
    }
    return corrupted;
  }

  /*
   * Corrupt a block on all datanode
   */
  void corruptBlockOnDataNodes(String blockName) throws Exception {
    for (int i = 0; i < dataNodes.size(); i++)
      corruptBlockOnDataNode(i, blockName);
  }

  /**
   * Returns the current set of datanodes
   */
  DataNode[] listDataNodes() {
    DataNode[] list = new DataNode[dataNodes.size()];
    for (int i = 0; i < dataNodes.size(); i++) {
      list[i] = dataNodes.get(i).datanode;
    }
    return list;
  }

  
  /**
   * getDatanodes from a particular namenode
   * @param index the namenode number
   * @return all the datanodes belong to the particular namenode
   */
  public ArrayList<DataNode> listDataNodesForOneNameNode(int index) {
    assert index < numNameNodes && index >= 0 : "index overflow";
    int namenodePort=getNameNodePort(index);
    ArrayList<DataNode> allDatanodes=getDataNodes();
    if(allDatanodes == null){
      return null;
    }
    ArrayList<DataNode> datanodes=new ArrayList<DataNode>();
    int size=allDatanodes.size();
    for(int i=0;i<size;i++){
      DataNode datanode=allDatanodes.get(i);
      if(datanode.getNameNodeAddr().getPort()==namenodePort){
        datanodes.add(datanode);
      }
    }
    return datanodes;
  }
  
  /**
   * get namenode which the datanode belong to,the result maybe null 
   * if datanode don't belong to any namenode  
   * @param datanode 
   * @return 
   */
  public NameNode getNameNodefromDataNode(DataNode datanode){
    for(int i=0;i<numNameNodes;i++){
      if(datanode.getNameNodeAddr().getPort() == 
        nameNodes.get(i).namenode.getNameNodeAddress().getPort()){
        return nameNodes.get(i).namenode;
      }
    }
    return null;
  }
  /**
   * Set the softLimit and hardLimit of client lease periods
   */
  void setLeasePeriod(long soft, long hard) {
    for (int i = 0; i < this.nameNodes.size(); i++) {
      NameNode namenode = this.nameNodes.get(i).namenode;
      namenode.namesystem.leaseManager.setLeasePeriod(soft, hard);
      namenode.namesystem.lmthread.interrupt();
    }
  }

  private String[] args(StartupOption operation) {
    boolean condition = operation == null || operation == StartupOption.FORMAT || operation == StartupOption.REGULAR;
    return condition ? new String[] {} : new String[] { operation.getName() };
  }

  // need to update
  private void formatNameIfNeed(boolean format) throws IOException {
    if (format) {
        tryDelete(dataDir);
        NameNode.format(namenodeconf);
      }
    }

  private String path(String name1) {
    return new File(getBaseDir(), name1).getPath();
  }

  private void setDefaultUrl(String url) {
    FileSystem.setDefaultUri(namenodeconf, url);
  }

  private void setDfsReplication(int numDataNodes) {
    int replication = namenodeconf.getInt("dfs.replication", 3);
    namenodeconf.setInt("dfs.replication", Math.min(replication, numDataNodes));
  }

  private void setNameDirIfNeed(boolean manageNameDfsDirs) {
    if (manageNameDfsDirs) {
      namenodeconf.set("dfs.name.dir", path("name1") + "," + path("name2"));
      namenodeconf.set("fs.checkpoint.dir", path("namesecondary1") + "," + path("namesecondary2"));
    }
  }

  private void setNodeSwitchMappingImpl(Class<DNSToSwitchMapping> face, Class<StaticMapping> impl) {
    this.namenodeconf.setClass("topology.node.switch.mapping.impl", impl, face);
  }

  private synchronized boolean shouldWait(DatanodeInfo[] dnInfo, boolean waitHeartbeats) {
    if (dnInfo.length < numDataNodes) { return true; }

    // If we don't need heartbeats, we're done.
    if (!waitHeartbeats) { return false; }

    // make sure all datanodes have sent first heartbeat to namenode,
    // using (capacity == 0) as proxy.
    for (DatanodeInfo dn : dnInfo) {
      if (dn.getCapacity() == 0) { return true; }
    }
    return false;
  }
  
  private static File getBaseDir() {
    return new File(System.getProperty("test.build.data", "build/test/data"), "dfs/");
  }
  
  public void waitDatanodeDie() {
    long heartbeatInterval =  namenodeconf.getLong("dfs.heartbeat.interval", 2) * 1000;
    long datanodeMapUpdateInterval = namenodeconf.getInt("datanodemap.update.interval", 5000);
    long datanodeExpireInterval = 2 * datanodeMapUpdateInterval + 10 * heartbeatInterval;
    try {
      Thread.sleep((1+this.nameNodes.size()) * datanodeExpireInterval);
    }catch (Exception e) {
    }
  }
  public void waitDatanodeReconnect(int NUM_OF_DATANODES) throws IOException {
    if (this.nameNodes.size() == 0) { return; }
    DatanodeInfo[] dnis = getDataNodeReportType(DatanodeReportType.LIVE);
    // make sure all datanodes are alive and sent heartbeat
    while (dnis.length != NUM_OF_DATANODES) {
      dnis = getDataNodeReportType(DatanodeReportType.LIVE);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
  }
}
