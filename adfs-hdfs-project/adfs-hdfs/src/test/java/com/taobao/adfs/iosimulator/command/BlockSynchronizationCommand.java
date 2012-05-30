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

package com.taobao.adfs.iosimulator.command;

import java.sql.ResultSet;

import org.apache.log4j.Logger;

import com.taobao.adfs.iosimulator.execute.IExecutor;
import com.taobao.adfs.iosimulator.execute.IExecutable.ExecuteType;
import com.taobao.adfs.iosimulator.state.StateBlock;
import com.taobao.adfs.iosimulator.state.StateFile;

public class BlockSynchronizationCommand extends AbstractCommand {
  protected Logger logger = Logger.getLogger(getClass().getName());
  protected long blockId;
  protected int datanodeId;
  
  public BlockSynchronizationCommand(IExecutor executor, long blockId, int datanodeId) {
    super(executor);
    this.blockId = blockId;
    this.datanodeId = datanodeId;
  }

  @Override
  public void execute() {

    boolean hasfile = false;
    try {
      StateFile sf;
      // 1. get stored block
      StateBlock sb = StateBlock.createStateBlock(blockId, datanodeId);
      ResultSet rs = (ResultSet) executor.run(ExecuteType.FINDBLOCKID, sb);
      if(rs.next()) {
        sb.loadStateBlock(rs);
        sf = StateFile.createStateFile(sb.getFileId());
      } else {
        sf = StateFile.createStateFile(0);
      }
      // 2. get file info
      rs = (ResultSet)executor.run(ExecuteType.FINDFILEID, sf);
      if(rs.next()) {
        hasfile = true;
        sf.loadStateFile(rs);
      }      
      // 3. remove place holder
      executor.run(ExecuteType.DELETEBLOCK, sb);
      // 4. add new one
      sb.updateDatanode(datanodeId);
      executor.run(ExecuteType.CREATEBLOCK, sb);
      if(hasfile) {
        // 5. update file
        sf.updateLength(0);
        executor.run(ExecuteType.UPDATEFILE, sf);
      }
      
      
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
}
