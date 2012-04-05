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

package com.taobao.adfs.iosimulator.execute;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;

import com.google.code.hs4j.FindOperator;
import com.google.code.hs4j.HSClient;
import com.google.code.hs4j.IndexSession;
import com.google.code.hs4j.impl.HSClientImpl;
import com.taobao.adfs.iosimulator.execute.IExecutable.ExecuteType;
import com.taobao.adfs.iosimulator.state.StateBlock;
import com.taobao.adfs.iosimulator.state.StateDatanode;
import com.taobao.adfs.iosimulator.state.StateFile;

public class HSExecutor implements IExecutor {
  
  private class SessionHandler {

    private final IndexSession session;
    private final int type;
    
    SessionHandler(IndexSession session, int type) {
      this.session = session;
      this.type = type;
    }
    
    public Object exec(String[] args, String[]...exargs) throws Exception {
      switch(type) {
      case 1:
        return session.insert(args);
      case 2:
        return session.find(args);
      case 3:
        return session.update(exargs[0], args, FindOperator.EQ);
      case 4:
        return session.delete(args);
      }
      return null;
    }
  }
  
  private static final String DBNAME = "nn_state";
  private static final String TABLE_FILE = "file";
  private static final String TABLE_BLOCK = "block";
  private static final String TABLE_DATANODE = "datanode";
  private static final String PRIMARY = "PRIMARY";
  private static final String PIDNAME = "PID_NAME";
  private static final String DNID = "DATANODE_ID";
  private static final String FLID = "FILE_ID";
  private static final int[] hstype = {1/*insert*/, 2/*find*/, 
      3/*update*/, 4/*delete*/};
  private HSClient hsWriter;
  private HSClient hsReader;
  private HashMap<ExecuteType, SessionHandler> handlers;

  
  public HSExecutor() throws Exception{
    hsWriter = new HSClientImpl(new InetSocketAddress(40003));
    hsReader = new HSClientImpl(new InetSocketAddress(40002));
    handlers = new HashMap<ExecuteType, SessionHandler>();
    setupHandlers();
  }
  
  private void setupHandlers() throws Exception {
    //initialize handlers for each execution types
    IndexSession priFileWriter = hsWriter.openIndexSession(DBNAME, TABLE_FILE,
        PRIMARY, StateFile.columns);
    IndexSession priFileReader = hsReader.openIndexSession(DBNAME, TABLE_FILE,
        PRIMARY, StateFile.columns);
    IndexSession pnmFileReader = hsReader.openIndexSession(DBNAME, TABLE_FILE,
        PIDNAME, StateFile.columns);   
    handlers.put(ExecuteType.CREATEFILE,  new SessionHandler(priFileWriter,1));
    handlers.put(ExecuteType.FINDFILEID,  new SessionHandler(priFileReader,2));
    handlers.put(ExecuteType.FINDPIDNAME, new SessionHandler(pnmFileReader,2));
    handlers.put(ExecuteType.UPDATEFILE, new SessionHandler(priFileWriter,3));
    handlers.put(ExecuteType.DELETEFILE, new SessionHandler(priFileWriter,4));
    IndexSession priDatanodeWriter = hsWriter.openIndexSession(DBNAME, TABLE_DATANODE,
        PRIMARY, StateDatanode.columns);
    IndexSession priDatanodeReader = hsReader.openIndexSession(DBNAME, TABLE_DATANODE,
        PRIMARY, StateDatanode.columns);
    handlers.put(ExecuteType.CREATEDATANODE, new SessionHandler(priDatanodeWriter,1));
    handlers.put(ExecuteType.FINDDATANODE, new SessionHandler(priDatanodeReader,2));
    handlers.put(ExecuteType.UPDATEDATANODE, new SessionHandler(priDatanodeWriter,3));
    handlers.put(ExecuteType.DELETEDATANODE, new SessionHandler(priDatanodeWriter,4));
    IndexSession priBlockWriter = hsReader.openIndexSession(DBNAME, TABLE_BLOCK,
        PRIMARY, StateBlock.columns);
    IndexSession priBlockReader = hsReader.openIndexSession(DBNAME, TABLE_BLOCK,
        PRIMARY, StateBlock.columns);
    IndexSession priBlockDnReader = hsReader.openIndexSession(DBNAME, TABLE_BLOCK,
        DNID, StateBlock.columns);
    IndexSession priBlockFlReader = hsReader.openIndexSession(DBNAME, TABLE_BLOCK,
        FLID, StateBlock.columns);
    handlers.put(ExecuteType.CREATEBLOCK, new SessionHandler(priBlockWriter,1));
    handlers.put(ExecuteType.FINDBLOCKID, new SessionHandler(priBlockReader,2));
    handlers.put(ExecuteType.FINDBLOCKBYFILE, new SessionHandler(priBlockFlReader,2));
    handlers.put(ExecuteType.FINDBLOCKBYDN, new SessionHandler(priBlockDnReader,2));
    handlers.put(ExecuteType.DELETEBLOCK, new SessionHandler(priBlockWriter,4));

  }
  
  
  @Override
  public Object run(final ExecuteType type, final IExecutable ea) 
  throws Exception {
    SessionHandler handler = handlers.get(type);
    return handler.exec(ea.toExecutable(type), ea.getFields(type));
  }
  
  public void close() {
    try {
      hsWriter.shutdown();
    } catch (IOException e) { }
    try {
      hsReader.shutdown();
    } catch (IOException e) { }
  }

}
