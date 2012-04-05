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

package com.taobao.adfs.iosimulator.command;

import java.sql.ResultSet;

import org.apache.log4j.Logger;

import com.taobao.adfs.iosimulator.execute.IExecutor;
import com.taobao.adfs.iosimulator.execute.IExecutable.ExecuteType;
import com.taobao.adfs.iosimulator.state.StateBlock;
import com.taobao.adfs.iosimulator.state.StateFile;

public class FileOpenCommand extends AbstractCommand {
  protected Logger logger = Logger.getLogger(getClass().getName());
  protected int fileId;
  
  public FileOpenCommand(IExecutor executor, int fileId) {
    super(executor);
    this.fileId = fileId;
  }

  @Override
  public void execute() {
    try {
      // 1. get file info
      StateFile sf = StateFile.createStateFile(fileId);
      ResultSet rs = (ResultSet) executor.run(ExecuteType.FINDFILEID, sf);
      if(rs.next()) {
        sf.loadStateFile(rs);
        // 2. getBlocks
        StateBlock sb = getBlocks(fileId);
        rs = (ResultSet) executor.run(ExecuteType.FINDBLOCKBYFILE, sb);
        long flen = 0;
        while(rs.next()) {
          sb.loadStateBlock(rs);
          flen += sb.getNumbytes();
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }   
  }
  
  private StateBlock getBlocks(int fileId) {
    long bid = -1;
    return StateBlock.createStateBlock(bid, -1, bid, fileId, -1);
  }
}
