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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.taobao.adfs.database.tdhsocket.client.TDHSClient;
import com.taobao.adfs.database.tdhsocket.client.TDHSClientImpl;
import com.taobao.adfs.database.tdhsocket.client.common.TDHSCommon;
import com.taobao.adfs.database.tdhsocket.client.request.ValueEntry;
import com.taobao.adfs.database.tdhsocket.client.response.TDHSResponse;
import com.taobao.adfs.iosimulator.execute.IExecutable.ExecuteType;

public class TDHExecutor implements IExecutor {
  
  private class ExecuteHandler {
    
    private final TDHSClient client;
    private final int type;
    private final String table;
    private final String key;
    
    ExecuteHandler(String tb, String k, int tp) {
      client = tdhClient;
      key = k;
      table = tb;
      type = tp;
    }
    
    
    public Object exec(String[] args, String[]...exargs) throws Exception {
      TDHSResponse response;
      switch(type) {
      case 1:
        response = client.insert(DBNAME, table, exargs[0], args);
        return response.getResultSet();
      case 2:
        response = client.get(DBNAME, table, key, exargs[0], new String[][]{args});
        return response.getResultSet();
      case 3:
        List<ValueEntry> valueEntries = new ArrayList<ValueEntry>(args.length);
        for (String value : args) {
            valueEntries.add(new ValueEntry(TDHSCommon.UpdateFlag.TDHS_UPDATE_SET, value));
        }
        response = client.update(DBNAME, table, key, exargs[0], 
            valueEntries.toArray(new ValueEntry[args.length]), new String[][]{exargs[1]}, 
            TDHSCommon.FindFlag.TDHS_EQ, 0, 1, null);
        return response.getResultSet();
      case 4:
        response = client.delete(DBNAME, table, key,
                  new String[][]{args},
                  TDHSCommon.FindFlag.TDHS_EQ, 0, 1, null);
        return response.getResultSet();
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
  private TDHSClient tdhClient;
  private HashMap<ExecuteType, ExecuteHandler> handlers;

  public TDHExecutor() throws Exception{
    int poolSize = 1;
    tdhClient = new TDHSClientImpl(new InetSocketAddress(40004), poolSize, 6000000);
    setupHandlers();
  }
  
  private void setupHandlers() throws Exception {
    handlers = new HashMap<ExecuteType, ExecuteHandler>();
    handlers.put(ExecuteType.CREATEFILE, new ExecuteHandler(TABLE_FILE, PRIMARY, 1));
    handlers.put(ExecuteType.FINDFILEID, new ExecuteHandler(TABLE_FILE, PRIMARY, 2));
    handlers.put(ExecuteType.FINDPIDNAME, new ExecuteHandler(TABLE_FILE, PIDNAME, 2));
    handlers.put(ExecuteType.UPDATEFILE, new ExecuteHandler(TABLE_FILE, PRIMARY, 3));
    handlers.put(ExecuteType.CREATEDATANODE, new ExecuteHandler(TABLE_DATANODE, PRIMARY, 1));
    handlers.put(ExecuteType.CREATEBLOCK, new ExecuteHandler(TABLE_BLOCK, PRIMARY, 1));
    handlers.put(ExecuteType.FINDBLOCKID, new ExecuteHandler(TABLE_BLOCK, PRIMARY, 2));
    handlers.put(ExecuteType.FINDBLOCKBYDN, new ExecuteHandler(TABLE_BLOCK, DNID, 2));
    handlers.put(ExecuteType.FINDBLOCKBYFILE, new ExecuteHandler(TABLE_BLOCK, FLID, 2));
    handlers.put(ExecuteType.DELETEBLOCK, new ExecuteHandler(TABLE_BLOCK, PRIMARY, 4));
  }
  
  @Override
  public void close() {
    tdhClient.shutdown();
  }
  
  @Override
  public Object run(ExecuteType type, IExecutable ea) throws Exception {
    ExecuteHandler handler = handlers.get(type);
    return handler.exec(ea.toExecutable(type), ea.getFields(type), ea.getKeys(type));
  }

}
