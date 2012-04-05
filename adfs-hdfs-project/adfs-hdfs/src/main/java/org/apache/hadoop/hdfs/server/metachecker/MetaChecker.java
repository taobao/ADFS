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

 package org.apache.hadoop.hdfs.server.metachecker;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.ZKClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.data.Stat;

import com.taobao.adfs.block.BlockEntry;
import com.taobao.adfs.state.*;

public class MetaChecker implements Tool, FSConstants{

  private static final Log LOG = 
    LogFactory.getLog(MetaChecker.class.getName());
  
  private static final Charset CHARSET = Charset.forName("UTF-8");
  private static final int MININTERVAL = 24*60; // minutes
  private static final int SECONDINTERVAL=24*60*60*1000;
  private static final String SLASH = "/";
  private static final String TOKEN = "token";
  private ZKClient.SimpleLock lock;
  
  private Configuration conf;
  private long datanodeExpireInterval;
  private final static Random rnd = new Random();
  
  // loop interval for loop mode
  private int interval;
  private boolean loopmode;
  private boolean forcemode;
  
  private StateManager stateManager;
  
  // Exit status
  final public static int SUCCESS = 0;
  final public static int ALREADY_RUNNING = -1;
  final public static int SHORT_TIME=-2;
  
  private Map<Long, SuspectedBlock> suspectedBlockMap;
  private Map<Integer,String> datanodeMap;
  private Map<String,Integer> storageMap;
  private DatanodeInfo[] liveDatanodes;
  static class SuspectedBlock {
    public final long blockId;
    public final int expectedRepNum;
    public final long numBytes;
    public final long generationStamp;
    public final int fileId;
    public final int index;
    public final List<Integer> datanodeIdList;
    
    public SuspectedBlock(long blkId, int repnum, 
        long nBytes, long gStamp, int fid, int idx, 
        List<Integer> dnIdList) {
      blockId = blkId;
      expectedRepNum = repnum;
      numBytes = nBytes;
      generationStamp = gStamp;
      fileId = fid;
      index = idx;
      datanodeIdList = dnIdList;
    }
    
    public boolean equals(Object o) {
      if (!(o instanceof SuspectedBlock)) {
        return false;
      }
      
      SuspectedBlock that = (SuspectedBlock)o;
      if(this == that) {
        return true;
      }
      
      int len = datanodeIdList.size();
      boolean ret = (blockId == that.blockId 
          && expectedRepNum == that.expectedRepNum
          && numBytes == that.numBytes
          && generationStamp == that.generationStamp
          && fileId == that.fileId
          && index == that.index
          && len == that.datanodeIdList.size());
      if(ret) {
        for(int i = 0; i < len; i++) {
          if (datanodeIdList.get(i) != that.datanodeIdList.get(i)) {
            ret = false;
          }
        }
      }
      return ret;
    }
    
    public int hashCode() {  
      final int prime = 31;  
      int ret = 1;
      ret = prime*ret + (int)blockId;
      ret = prime*ret + expectedRepNum;
      ret = prime*ret + (int)numBytes;
      ret = prime*ret + (int)generationStamp;
      ret = prime*ret + fileId;
      ret = prime*ret + index;
      for(int dnId : datanodeIdList) {
        ret = prime*ret + dnId;
      }
      return ret;
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    try {
      init(args);
      if (lock.trylock()) {
        try {
          return doWork();
        } finally {
          lock.unlock();
        }
      } else {
        LOG.info("Another block checker is running. Exiting...");
        return ALREADY_RUNNING;
      }
    } finally {
      cleanup();
    }
  }
  
  private int doWork() throws Exception {
    do {
      if (!loopmode) {
        if (withinLimitedTime()) {
          return SHORT_TIME;
        }
      }
      try {
        handleMetaChecker();
        LOG.info("checking blocks completed!");
      } catch (IOException e) {
        LOG.info("checking blocks failed!");
      }
      if (loopmode) {
        LOG.info("Sleep a while for another loop!");
        try {
          TimeUnit.MINUTES.sleep(interval);
        } catch (InterruptedException e) {}
      }
    } while (loopmode);
    return SUCCESS;
  }
  
  /**
   * This function tries to find suspect corrupted blocks, and 
   * mark them for further handling. 
   * @throws IOException
   */
  private void findSuspectBlock() throws IOException{
    Collection<DatanodeInfo> liveDatanodeColl= stateManager.getAliveDatanodes(datanodeExpireInterval);
    if(liveDatanodeColl == null) return;
    LOG.info("got live datanode collection from  blockmap");
    liveDatanodes=liveDatanodeColl.toArray(new DatanodeInfo[0]);
    shuffleArray(liveDatanodes);
    LOG.info("shuffled randomly the live datanodes");
    for(DatanodeInfo dnInfo : liveDatanodes) {
      Collection<BlockEntry> beList = null;
      datanodeMap.put(dnInfo.getNodeid(), dnInfo.getStorageID());
      storageMap.put(dnInfo.getStorageID(), dnInfo.getNodeid());
      try {
        beList = stateManager.getAllBlocksOnDatanode(dnInfo.getNodeid());
      } catch (IOException e) {
        LOG.warn("IOException occurs when get blocks from " + dnInfo.getName());
        beList = null;
        continue;
      }
      if(beList == null) continue;
      LOG.info("got all block entries of datanode: " + dnInfo.getName());
      for(BlockEntry be : beList) {
        long blkId = be.getBlockId();
        BlockEntry storedBe = null;
        try {
          storedBe = stateManager.getStoredBlockBy(blkId);
        } catch(IOException e) {
          LOG.warn("IOException occurs when getStoredBlockBy for block " 
              + blkId);
          storedBe = null;
          continue;
        }
        List<Integer> dnIdList = null;
        if(storedBe != null) {
          dnIdList = storedBe.getDatanodeIds();
        } else {
          LOG.warn("Block " + blkId 
              + " is missing when callign getStoredBlockBy(blkId");
          continue;
        }
        boolean isUnderWriting = 
          be.getNumbytes()==0 && (dnIdList.size()==1 && dnIdList.get(0)==-1);
        FileStatus fstatus = null;
        try {
          fstatus = 
            StateManager.fileToFileStatus(stateManager.getFileInfo(be.getFileId()));
        } catch(IOException e) {
          fstatus = null;
          LOG.warn("Block " + blkId 
              + " file info cannot be attained");
          continue;
        }
        if(fstatus == null) {
          LOG.warn("Block " + blkId + " has no corresponding file! Just remove it. ");
          try {
            stateManager.removeBlock(blkId);
          } catch (Exception e) {
            LOG.warn("Error happened when trying to remove block " + 
                      blkId + " from StateManager. ");
          }
          continue;
        }
        int rep = fstatus.getReplication();
        if(!isUnderWriting && rep != dnIdList.size()) {
          LOG.info("Suspected Block: " + blkId 
              + " <expect=" + rep + ", current=" + dnIdList.size() + ">");
          boolean canput = false;
          if (!suspectedBlockMap.containsKey(blkId)) {
            canput = true;
          } else {
            SuspectedBlock stored = suspectedBlockMap.get(blkId);
            // replace the old one if already exists
            if (stored.generationStamp <= be.getGenerationStamp()) {
              canput = true;
            } else {
              LOG.info(blkId + " is already in " +
                  "suspectedBlockMap and newer than the comming one");
            }
          }
          if(canput) {
            suspectedBlockMap.put(blkId, 
                new SuspectedBlock(blkId, rep, 
                    be.getNumbytes(), be.getGenerationStamp(), 
                    be.getFileId(), be.getIndex(), dnIdList));
            LOG.info(blkId + " is added into suspectedBlockMap");
          }
          
        } else {
          
          if(isUnderWriting) {
            LOG.info("Block: " + blkId + " is under writing.");
          }
          // a good block now, should be remove from suspectedBlockMap if exists
          if (suspectedBlockMap.containsKey(blkId)) {
            suspectedBlockMap.remove(blkId);
            LOG.info(blkId + " is removed from suspectedBlockMap");
          }
        }
      } // for be
      try {
        // don't go too hurry
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException ignored) {
      }
    } // for dnInfo
  }

  /**
   * Namenodes may fail to delete block replications after a 
   * datanode goes down, sometimes because a Namenode was corrupted. 
   * This function removes these block replications directly in state. 
   * @throws IOException
   */
  private void removeDeadBlock4DeadDN() throws IOException {
    Collection<DatanodeInfo> allDeadNodes = stateManager.getAliveDatanodes(Long.MAX_VALUE);
    Collection<DatanodeInfo> possibleAliveDatanodes= stateManager.getAliveDatanodes(datanodeExpireInterval*2);
    if(allDeadNodes == null || possibleAliveDatanodes == null) return;
    for(DatanodeInfo dn : possibleAliveDatanodes) {
      allDeadNodes.remove(dn);
    }
    for(DatanodeInfo dn : allDeadNodes){
      Collection<BlockEntry> beList = null;
      int dnID = dn.getNodeid();
      try {
        beList = stateManager.getAllBlocksOnDatanode(dnID);
      } catch (IOException e) {
        LOG.warn("IOException occurs when get blocks from " + dn.getName());
        beList = null;
        continue;
      }
      if(beList == null) continue;
      LOG.info("got all block entries of dead datanode " + dn.getName());
      for(BlockEntry be : beList) {
        if(be != null) {
          try {
            stateManager.removeBlockReplicationOnDatanodeBy(dnID, be.getBlockId());
          } catch (IOException e) {
            LOG.warn("IOException occurs when trying to remove block"+
                      be.getBlockId()+" from " + dn.getName());
          }
        }
      }
      try {
        // don't continue too hurry
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException ignored) {
      }
    } // end of for loop allDeadNodes
  }
  
  private void handleSuspeckBlock(){
    LOG.info("handle all the SuspectedBlock...");
    for(SuspectedBlock sb : suspectedBlockMap.values()) {
      long blkId = sb.blockId;
      long numBytes = sb.numBytes;
      long generationStamp = sb.generationStamp;
      List<Integer> dnList = sb.datanodeIdList;
      int delta = sb.expectedRepNum - sb.datanodeIdList.size();
      boolean isUnderDelete=false;
      if(delta < 0) {
        LOG.info(sb.blockId + " is over replication");
        delta = -1*delta;
        try {
          /* delete only one block entry is enough to trigger
             subsequence block replication actions in name node */
          int del = rnd.nextInt(sb.datanodeIdList.size());
          StringBuilder sber = new StringBuilder(FSConstants.ZOOKEEPER_EXCESS_HOME);
          sber.append(SLASH);
          int mark = sber.length();
          for(Integer dnId:dnList){
            String storageID=datanodeMap.get(dnId);
            if(storageID==null) continue;
            sber.append(storageID).append(SLASH).append(blkId);
            Stat stat=ZKClient.getInstance().exist(sber.toString());
            sber.delete(mark, sber.length());
            if(stat!=null){
              LOG.info("The block may be under delete, check it next time");
              isUnderDelete=true;
              break;
            }
          }
          
          if(!isUnderDelete){
            stateManager.removeBlockReplicationOnDatanodeBy(dnList.get(del), blkId);
            LOG.info("   <blockid=" + 
                       blkId + ",datanodeid=" + dnList.get(del) + "> in blockmap");
          }
        } catch (IOException e) {
          LOG.warn("IOException occurs when calling " +
              "removeBlockReplicationOnDatanodeBy for Block " + blkId);
          continue;
        }
      } else if (delta > 0){
        LOG.info(sb.blockId + " is under replication");
        DatanodeInfo candidate = getCandidateDatanode(liveDatanodes, dnList);
        if(candidate != null) {
          try {
            /* add only one block entry is enough to trigger
            subsequence block replication actions in name node */
            stateManager.receiveBlockFromDatanodeBy(candidate.getNodeid(), blkId, numBytes, generationStamp);
            LOG.info("added a faked record of <blockid=" + 
                blkId + ",datanodeid=" + candidate.getNodeid() + "> in blockmap");
          }catch(IOException e) {
            LOG.warn("IOException occurs when calling " +
                "receiveBlockFromDatanodeBy for Block " + blkId);
            continue;           
          }
        } else {
          LOG.warn("Cannot find any candidate datanode for adding a faked record");
        }
      }
    }   
    LOG.info("handle all the SuspectedBlock completed!");
  }
  
  /**
   * the method why we just handle invalidateset is
   * because on FSNamesystem we may crash in removeStoredBlock function.
   * if that happen , the excess,corrupt and invalidate may be inconsistent with db,
   * invalidate is the last one to process in the function,
   * so we just handle it and delete all in zookeeper to correct the wrong logic.
   * 
   * @throws IOException
   */
  private void checkZookeeper() throws IOException{
    LOG.info("Now ,check the status of zookeeper with DB");
    List<String> invalidateStorageIds=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_INVALIDATE_HOME, null);
    if(invalidateStorageIds!=null){
      for(String storageId:invalidateStorageIds){
        List<String> blockIds=ZKClient.getInstance().getChildren(FSConstants.ZOOKEEPER_INVALIDATE_HOME
                                                   +SLASH+storageId, null);
        if(blockIds==null || blockIds.size()==0){
          ZKClient.getInstance().delete(FSConstants.ZOOKEEPER_INVALIDATE_HOME+SLASH+storageId, false);
          continue;
        }
        if(storageMap.get(storageId)==null) continue;
        int datanodeId=storageMap.get(storageId);
        StringBuilder sber = new StringBuilder();
        for(String blockId:blockIds){
          BlockEntry be = stateManager.getStoredBlockBy(Long.parseLong(blockId));
          boolean isInvalidateWrong=false;
          if(be==null){
            isInvalidateWrong=true;
          }else{
            List<Integer> dnList = be.getDatanodeIds();
            if(!dnList.contains(datanodeId)){
              isInvalidateWrong=true;
            }
          }
          if(isInvalidateWrong){
            sber.append(FSConstants.ZOOKEEPER_EXCESS_HOME).append(SLASH).append(storageId)
            .append(SLASH).append(blockId);
            ZKClient.getInstance().delete(sber.toString(), true);
            sber.delete(0, sber.length());
            sber.append(FSConstants.ZOOKEEPER_CORRUPT_HOME).append(SLASH).append(blockId)
            .append(SLASH).append(storageId);
            ZKClient.getInstance().delete(sber.toString(), true);
            sber.append(FSConstants.ZOOKEEPER_INVALIDATE_HOME).append(SLASH).append(storageId)
            .append(SLASH).append(blockId);            
            ZKClient.getInstance().delete(sber.toString(), true);
            sber.delete(0, sber.length());
          }
        }
      }
    }
    LOG.info("handle all the zookeeper node");
  }
  
  private void handleMetaChecker() throws IOException {
    removeDeadBlock4DeadDN();
    findSuspectBlock();
    handleSuspeckBlock();
    checkZookeeper(); 
  }
  
  /* Shuffle datanode array */
  static private void shuffleArray(DatanodeInfo[] datanodes) {
    for (int i=datanodes.length; i>1; i--) {
      int randomIndex = rnd.nextInt(i);
      DatanodeInfo tmp = datanodes[randomIndex];
      datanodes[randomIndex] = datanodes[i-1];
      datanodes[i-1] = tmp;
    }
  }
  
  static private DatanodeInfo getCandidateDatanode(DatanodeInfo[] datanodes, List<Integer> exclusive) {
    for (int i=datanodes.length; i>1; i--) {
      int randomIndex = rnd.nextInt(i);
      DatanodeInfo candidate = datanodes[randomIndex];
      Iterator<Integer> it = exclusive.iterator();
      boolean hit = false;
      while(it.hasNext()) {
        if(it.next() == candidate.getNodeid()) {
          hit = true;
          break;
        }
      }
      if(!hit) {
        return candidate;
      } else {
        datanodes[randomIndex] = datanodes[i-1];
        datanodes[i-1] = candidate;    
      }
    }
    return null;
  }
  
  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.conf.addResource("hdfs-site.xml");
  }
  
  /** 
   * Initialize meta checker. 
   * It sets the value of the interval, and fs handler
   */
  private void init(String[] args) throws Exception {
    parseArgs(args);
    this.stateManager = new StateManager(conf);
    suspectedBlockMap = new HashMap<Long, SuspectedBlock>();
    datanodeMap=new HashMap<Integer,String>();
    storageMap=new HashMap<String,Integer>();
    long heartbeatInterval = conf.getLong("dfs.heartbeat.interval", 3) * 1000; 
    long datanodeMapUpdateInterval = conf.getInt("datanodemap.update.interval", 5 * 60 * 1000);
    datanodeExpireInterval = 2 * datanodeMapUpdateInterval + 10 * heartbeatInterval;
    ZKClient.getInstance().create(ZOOKEEPER_METACHECKER_HOME, new byte[0], false, true);
    lock = ZKClient.getInstance().getSimpleLock(ZOOKEEPER_METACHECKER_HOME, TOKEN);
    LOG.info("MetaChecker initialized!");
  }
  
  private boolean withinLimitedTime() {
    StringBuilder sb = new StringBuilder();
    try {
      byte[] data = ZKClient.getInstance().getData(ZOOKEEPER_METACHECKER_HOME, null);
      if (data == null) {
        sb.append(System.currentTimeMillis());
        ZKClient.getInstance().setData(ZOOKEEPER_METACHECKER_HOME,
            sb.toString().getBytes(CHARSET));
      } else {
        String strData = new String(data, CHARSET);
        long lastTime = -1;
        try {
          lastTime = Long.parseLong(strData);
        } catch(NumberFormatException ignored) {}
        if (System.currentTimeMillis() - lastTime < SECONDINTERVAL
            && !forcemode) {
          LOG.info("The time is shorter then 24 hours,try it later or use -force parameter");
          return true;
        } else {
          sb.append(System.currentTimeMillis());
          ZKClient.getInstance().setData(ZOOKEEPER_METACHECKER_HOME, 
              sb.toString().getBytes(CHARSET));
        }
      }
    } catch (IOException e) {
      LOG.warn("Error when connect to zookeeper");
      return true;
    }
    return false;
  }
  
  private void cleanup() {
    
    // first, cleanup the collection in memory
    if(suspectedBlockMap != null) {
      suspectedBlockMap.clear();
    }
    if(datanodeMap != null) {
      datanodeMap.clear();
    }
    if(storageMap != null) {
      storageMap.clear();
    }
    liveDatanodes = null;
    
    // second, cleanup the remote handlers
    try {
      ZKClient.getInstance().close();
    } catch (IOException e) {
      LOG.error("exception occurs when close ZKClient.", e);
    }

    if (stateManager != null) {
      try {
        stateManager.close();
      } catch (IOException e) { 
        LOG.error("exception occurs when close stateManager.", e);
      }
    }
    LOG.info("Cleanup job for MetaChecker completed!");
  }
  
  /** Default constructor */
  MetaChecker() {}
  
  /** Construct a MetaChecker from the given configuration */
  MetaChecker(Configuration conf) {
    setConf(conf);
  } 

  /** Construct a MetaChecker from the given configuration and interval */
  MetaChecker(Configuration conf, int interval) {
    setConf(conf);
    this.interval = interval;
  }
  
  
  /** parse argument to get the interval, if no interval value is passed in, the block
   * checker will turn into run-once-mode
  */
  private void parseArgs(String[] args) {
    int argsLen = (args == null) ? 0 : args.length;
    if (argsLen==0) {
      loopmode = false;
      forcemode = false;
      LOG.info( "In run once mode, will run checker only once" );
    } else {
      if(argsLen == 1){
        if (!"-force".equalsIgnoreCase(args[0])){
          printUsage();
          throw new IllegalArgumentException(Arrays.toString(args));
        } else {
          forcemode=true;
          LOG.info("In force mode, force to run metaChecker");
        }
      } else if(argsLen == 2){
        if(!"-interval".equalsIgnoreCase(args[0])){
          printUsage();
          throw new IllegalArgumentException(Arrays.toString(args));
        } else {
          try {
            int input = Integer.parseInt(args[1]);
            if (input < MININTERVAL) {
              throw new NumberFormatException();
            }
            loopmode = true;
            forcemode = false;
            interval = input;
            LOG.info( "In loop mode, using an interval of " + interval + " minutes");
          } catch(NumberFormatException e) {
            System.err.println(
                "Expect an interval no less than the minimum value: "+ MININTERVAL);
            printUsage();
            throw e;
          }
        }
      } else {
        printUsage();
        throw new IllegalArgumentException(Arrays.toString(args));
      }
    }
  }
  
  private static void printUsage() {
    System.out.println("Usage: java MetaChecker\t");
    System.out.println("          [-interval <interval>]\t" 
        +"run metachecker each <interval> minutes");
    System.out.println("          [-force <force>]\t"
        +"force to run metachecker");
  }
  
  public static void main(String[] args) {
    try {
      System.exit( ToolRunner.run(null, new MetaChecker(), args) );
    } catch (Throwable e) {
      LOG.error(StringUtils.stringifyException(e));
      System.exit(-1);
    }

  }
}
