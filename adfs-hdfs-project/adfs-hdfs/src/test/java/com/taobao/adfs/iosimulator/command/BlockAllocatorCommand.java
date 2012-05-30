/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 \*   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.taobao.adfs.iosimulator.command;

import org.apache.log4j.Logger;

import com.taobao.adfs.iosimulator.execute.IExecutor;
import com.taobao.adfs.iosimulator.execute.IExecutable.ExecuteType;
import com.taobao.adfs.iosimulator.state.StateBlock;
import com.taobao.adfs.iosimulator.state.StateFile;

public class BlockAllocatorCommand extends AbstractCommand {
  
  private Logger logger = Logger.getLogger(getClass().getName());
  private long blkId;
  private int dnId;
  private long gs;
  private int fileId;
  private int fileIndex;

  BlockAllocatorCommand(IExecutor executor) {
    super(executor);
  }
  
  public BlockAllocatorCommand(IExecutor executor, long blkId, int dnId,
      long gs, int fileId, int fileIndex) {
    super(executor);
    this.blkId = blkId;
    this.dnId = dnId;
    this.gs = gs;
    this.fileId = fileId;
    this.fileIndex = fileIndex;
  }


  @Override
  public void execute() {
    try {
      // 1. get file info
      StateFile sf = StateFile.createStateFile(fileId);
      executor.run(ExecuteType.FINDFILEID, sf);
      
      StateBlock sb = newBlock();
      // 2. get blocks by fileId
      executor.run(ExecuteType.FINDBLOCKBYFILE, sb);
      // 3. allocate block
      executor.run(ExecuteType.CREATEBLOCK, sb);
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private StateBlock newBlock() {
    return StateBlock.createStateBlock(blkId, dnId, gs, 
        fileId, fileIndex);
  }

}
