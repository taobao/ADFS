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
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.ExternalNamenodeProtocol;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

/**
 * This is a common handler to handle commands belong to external data node
 */
public class ExternalDatanodeCommandsHandler {

  public static final Log LOG = LogFactory
      .getLog(ExternalDatanodeCommandsHandler.class);

  private FSNamesystem fsNamesystem;
  private Configuration conf;

  /**
   * Cache the IPC address of external data node and corresponding protocol pair
   * InetSocketAddress : remote datanode address (host:ipc-port)
   * ExternalNamenodeProtocol: proxy handler of the target external datanode
   */
  private ConcurrentHashMap<InetSocketAddress, ExternalNamenodeProtocol> proxyCache;

  /**
   * A private internal class to describe the pair of IPC address and data node command
   * InetSocketAddress : remote datanode address (host:ipc-port)
   * DatanodeCommand: data node command to be executed
   */
  private static class DatanodeAndCommandPair {
    public DatanodeAndCommandPair(InetSocketAddress addr, DatanodeCommand cmd) {
      ipcAddress = addr;
      dnCommand = cmd;
    }

    // should be faster to access directly
    public InetSocketAddress ipcAddress;
    public DatanodeCommand dnCommand;
  }

  private ThreadFactory commandSenderThreads;

  private ExecutorService commandSenderExecutor;

  private class CommandSender implements Runnable {

    private final InetSocketAddress isa;
    private final DatanodeCommand cmd;

    public CommandSender(DatanodeAndCommandPair dcPair) {
      super();
      isa = dcPair.ipcAddress;
      cmd = dcPair.dnCommand;
    }

    @Override
    public void run() {
      ExternalNamenodeProtocol protocol, exist = null;
      boolean retried = false;
      do {    
        // get proxy if not cached
        if((protocol = proxyCache.get(isa)) == null) {
          try {
            protocol = 
              (ExternalNamenodeProtocol)RPC.getProxy(ExternalNamenodeProtocol.class, 
                  ExternalNamenodeProtocol.versionID, isa, conf);
          } catch (IOException e) {
            LOG.error("Cannot get protocol proxy of " + 
                isa.getHostName() + ", abort this sender task");
            fsNamesystem.removeExternalDatanode(isa);
            break; // cannot get proxy, have to exit
          }
        }        
        // send command
        try {
          protocol.sendCommand(cmd);
          LOG.info("send out command to " + isa.getHostName());
          // always store a good one, only one
          exist = proxyCache.putIfAbsent(isa, protocol);
          if(exist != null && exist != protocol) {
            // this means 'protocol' is not the one stored in proxyCache
            RPC.stopProxy(protocol); 
          }
          break; // normal exit
        } catch (IOException e) {
          LOG.error("Cannot send command to " + isa.getHostName());
          if (retried) { // retried, but still failed
            RPC.stopProxy(protocol);
            break; 
          } else { // oops... let's clean up and retry once
            proxyCache.remove(isa);
            protocol = null;
            retried = true;           
          }
        }
      } while(true);
    }
  }

  public ExternalDatanodeCommandsHandler(FSNamesystem fsNamesystem, Configuration conf) {
    this.fsNamesystem = fsNamesystem;
    if (conf != null) {
      this.conf = conf;
    } else {
      this.conf = new Configuration();
    }
    init();
  }

  private void init() {
    LOG.info("starts initialization process...");
    proxyCache = new ConcurrentHashMap<InetSocketAddress, ExternalNamenodeProtocol>();
    commandSenderThreads = new ThreadFactory() {
      private final String threadFamily = "cmdsender-";
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName(threadFamily + thread.getId());
        return thread;
      }
    };
    commandSenderExecutor = new ThreadPoolExecutor(1, fsNamesystem.PROCESSORS, 60L, TimeUnit.SECONDS, 
        new SynchronousQueue<Runnable>(), commandSenderThreads,
        new ThreadPoolExecutor.CallerRunsPolicy());
    LOG.info("initialization process completed");
  }

  public void transferBlock(final DatanodeDescriptor dnDesc, final int size) {
    if(dnDesc == null) return;
    addCommand(extractIpcAddress(dnDesc), dnDesc.getReplicationCommand(size));
  }
  
  public void invalidateBlocks(final DatanodeDescriptor dnDesc, final int size) {
    if(dnDesc == null) return;
    addCommand(extractIpcAddress(dnDesc), dnDesc.getInvalidateBlocks(size));
  }
  
  public void recoverBlocks(final DatanodeDescriptor dnDesc, final int size) {
    if(dnDesc == null) return;
    addCommand(extractIpcAddress(dnDesc), dnDesc.getLeaseRecoveryCommand(size));
  }

  public void removeIfExists(final DatanodeDescriptor dnDesc) {
    if(dnDesc == null) return;
    InetSocketAddress isa = extractIpcAddress(dnDesc);
    ExternalNamenodeProtocol enp = proxyCache.remove(isa);
    if (enp != null) {
      RPC.stopProxy(enp);
    }
  }

  public void stop() {
    cleanup();
  }

  private InetSocketAddress extractIpcAddress(DatanodeDescriptor dnDesc) {
    StringBuilder sb = new StringBuilder(dnDesc.getHost());
    sb.append(":").append(dnDesc.getIpcPort()); // we must use the IPC port
    InetSocketAddress ipcAddress = NetUtils.createSocketAddr(sb.toString());
    return ipcAddress;
  }

  private void addCommand(InetSocketAddress ipcAddress, DatanodeCommand dnCmd) {
    DatanodeAndCommandPair dcPair = new DatanodeAndCommandPair(ipcAddress,
        dnCmd);
    try {
      commandSenderExecutor.execute(new CommandSender(dcPair));
    } catch(RejectedExecutionException ree) {
      LOG.error("commandSenderExecutor refused to add more commands");
    }
  }

  private void cleanup() {
    LOG.info("clean up process starts...");
    commandSenderExecutor.shutdown();
    synchronized (proxyCache) {
      Iterator<ExternalNamenodeProtocol> it = proxyCache.values().iterator();
      while (it.hasNext()) {
        RPC.stopProxy(it.next());
      }
      proxyCache.clear();
    }
    try {
      commandSenderExecutor.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("commandSenderExecutor interrupted!");
    }
    LOG.info("clean up process completed!");
  }
}