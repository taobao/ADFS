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

package org.apache.hadoop.hdfs.server.protocol;

import java.io.*;

import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.AccessControlException;

/**********************************************************************
 * Protocol that a DFS datanode uses to communicate with the NameNode.
 * It's used to upload current load information and block reports.
 *
 * The only way a NameNode can communicate with a DataNode is by
 * returning values from these functions.
 *
 **********************************************************************/
public interface BlockManagementProtocol extends VersionedProtocol {
  /**
   * 19: SendHeartbeat returns an array of DatanodeCommand objects
   *     in stead of a DatanodeCommand object.
   */
  public static final long versionID = 19L;
  
  // error code
  final static int NOTIFY = 0;
  final static int DISK_ERROR = 1; // there are still valid volumes on DN
  final static int INVALID_BLOCK = 2;
  final static int FATAL_DISK_ERROR = 3; // no valid volumes left on DN

  /**
   * Determines actions that data node should perform 
   * when receiving a datanode command. 
   */
  final static int DNA_UNKNOWN = 0;    // unknown action   
  final static int DNA_TRANSFER = 1;   // transfer blocks to another datanode
  final static int DNA_INVALIDATE = 2; // invalidate blocks
  final static int DNA_SHUTDOWN = 3;   // shutdown node
  final static int DNA_REGISTER = 4;   // re-register
  final static int DNA_FINALIZE = 5;   // finalize previous upgrade
  final static int DNA_RECOVERBLOCK = 6;  // request a block recovery

  /** 
   * Register Datanode.
   *
   * @see org.apache.hadoop.hdfs.server.datanode.DataNode#dnRegistration
   * @see org.apache.hadoop.hdfs.server.namenode.FSNamesystem#registerDatanode(DatanodeRegistration)
   * 
   * @return updated {@link org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration}, which contains 
   * new storageID if the datanode did not have one and
   * registration ID for further communication.
   */
  public DatanodeRegistration register(DatanodeRegistration registration
                                       ) throws IOException;
  /**
   * sendHeartbeat() tells the NameNode that the DataNode is still
   * alive and well.  Includes some status info, too. 
   * It also gives the NameNode a chance to return 
   * an array of "DatanodeCommand" objects.
   * A DatanodeCommand tells the DataNode to invalidate local block(s), 
   * or to copy them to other DataNodes, etc.
   */
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration registration,
                                       long capacity,
                                       long dfsUsed, long remaining,
                                       int xmitsInProgress,
                                       int xceiverCount) throws IOException;

  /**
   * blockReport() tells the NameNode about all the locally-stored blocks.
   * The NameNode returns an array of Blocks that have become obsolete
   * and should be deleted.  This function is meant to upload *all*
   * the locally-stored blocks.  It's invoked upon startup and then
   * infrequently afterwards.
   * @param registration
   * @param blocks - the block list as an array of longs.
   *     Each block is represented as 2 longs.
   *     This is done instead of Block[] to reduce memory used by block reports.
   *     
   * @return - the next command for DN to process.
   * @throws IOException
   */
  public DatanodeCommand blockReport(DatanodeRegistration registration,
                                     long[] blocks) throws IOException;
    
  /**
   * blockReceived() allows the DataNode to tell the NameNode about
   * recently-received block data, with a hint for pereferred replica
   * to be deleted when there is any excessive blocks.
   * For example, whenever client code
   * writes a new Block here, or another DataNode copies a Block to
   * this DataNode, it will call blockReceived().
   */
  public void blockReceived(DatanodeRegistration registration,
                            Block blocks[],
                            String[] delHints) throws IOException;

  /**
   * errorReport() tells the NameNode about something that has gone
   * awry.  Useful for debugging.
   */
  public void errorReport(DatanodeRegistration registration,
                          int errorCode, 
                          String msg) throws IOException;
    
  public NamespaceInfo versionRequest() throws IOException;

  /**
   * This is a very general way to send a command to the name-node during
   * distributed upgrade process.
   * 
   * The generosity is because the variety of upgrade commands is unpredictable.
   * The reply from the name-node is also received in the form of an upgrade 
   * command. 
   * 
   * @return a reply in the form of an upgrade command
   */
  UpgradeCommand processUpgradeCommand(UpgradeCommand comm) throws IOException;
  
  /**
   * same as {@link org.apache.hadoop.hdfs.protocol.ClientProtocol#reportBadBlocks(LocatedBlock[])}
   * }
   */
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException;
  
  /**
   * @return the next GenerationStamp to be associated with the specified
   * block. 
   */
  public long nextGenerationStamp(Block block) throws IOException;

  /**
   * Commit block synchronization in lease recovery
   */
  public void commitBlockSynchronization(Block block,
      long newgenerationstamp, long newlength,
      boolean closeFile, boolean deleteblock, DatanodeID[] newtargets
      ) throws IOException;
  
  
  ///////////////////////////////////////
  // File contents
  ///////////////////////////////////////
  /**
   * Get locations of the blocks of the specified file within the specified range.
   * DataNode locations for each block are sorted by
   * the proximity to the client.
   * <p>
   * Return {@link LocatedBlocks} which contains
   * file length, blocks and their locations.
   * DataNode locations for each block are sorted by
   * the distance to the client's address.
   * <p>
   * The client will then have to contact 
   * one of the indicated DataNodes to obtain the actual data.
   * 
   * @param src file name
   * @param offset range start offset
   * @param length range length
   * @return file length and array of blocks with their locations
   * @throws IOException
   */
  public LocatedBlocks  getBlockLocations(String src,
                                          long offset,
                                          long length) throws IOException;
  
  /**
   * Append to the end of the file. 
   * @param src path of the file being created.
   * @param clientName name of the current client.
   * @return information about the last partial block if any.
   * @throws AccessControlException if permission to append file is 
   * denied by the system. As usually on the client side the exception will 
   * be wrapped into {@link org.apache.hadoop.ipc.RemoteException}.
   * Allows appending to an existing file if the server is
   * configured with the parameter dfs.support.append set to true, otherwise
   * throws an IOException.
   * @throws IOException if other errors occur.
   */
  public LocatedBlock append(String src, String clientName) throws IOException;
  
  /**
   * The client can give up on a blcok by calling abandonBlock().
   * The client can then
   * either obtain a new block, or complete or abandon the file.
   * Any partial writes to the block will be discarded.
   */
  public void abandonBlock(Block b, String src, String holder
      ) throws IOException;
  
  /**
   * A client that wants to write an additional block to the 
   * indicated filename (which must currently be open for writing)
   * should call addBlock().  
   *
   * addBlock() allocates a new block and datanodes the block data
   * should be replicated to.
   * 
   * @deprecated use the 3-arg form below
   * @return LocatedBlock allocated block information.
   */
  public LocatedBlock addBlock(String src, String clientName) throws IOException;
  
  /**
   * A client that wants to write an additional block to the 
   * indicated filename (which must currently be open for writing)
   * should call addBlock().  
   *
   * addBlock() allocates a new block and datanodes the block data
   * should be replicated to.
   *
   * @param excludedNodes a list of nodes that should not be allocated
   * 
   * @return LocatedBlock allocated block information.
   */
  public LocatedBlock addBlock(String src, String clientName,
                               DatanodeInfo[] excludedNodes) throws IOException;
  
  
  ///////////////////////////////////////
  // System issues and management
  ///////////////////////////////////////

  /**
   * Client programs can cause stateful changes in the NameNode
   * that affect other clients.  A client may obtain a file and 
   * neither abandon nor complete it.  A client might hold a series
   * of locks that prevent other clients from proceeding.
   * Clearly, it would be bad if a client held a bunch of locks
   * that it never gave up.  This can happen easily if the client
   * dies unexpectedly.
   * <p>
   * So, the NameNode will revoke the locks and live file-creates
   * for clients that it thinks have died.  A client tells the
   * NameNode that it is still alive by periodically calling
   * renewLease().  If a certain amount of time passes since
   * the last call to renewLease(), the NameNode assumes the
   * client has died.
   */
  public void renewLease(String clientName) throws IOException;
  
  /**
   * Get the block size for the given file.
   * @param filename The name of the file
   * @return The number of bytes in each block
   * @throws IOException
   */
  public long getPreferredBlockSize(String filename) throws IOException;
  
  
  /**
   * The client is done writing data to the given filename, and would 
   * like to complete it.  
   *
   * The function returns whether the file has been closed successfully.
   * If the function returns false, the caller should try again.
   *
   * A call to complete() will not return true until all the file's
   * blocks have been replicated the minimum number of times.  Thus,
   * DataNode failures may cause a client to call complete() several
   * times before succeeding.
   */
  public boolean complete(String src, String clientName) throws IOException;
}
