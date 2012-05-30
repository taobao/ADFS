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
import java.util.Random;

import org.apache.log4j.Logger;

import com.taobao.adfs.iosimulator.execute.IExecutor;
import com.taobao.adfs.iosimulator.execute.IExecutable.ExecuteType;
import com.taobao.adfs.iosimulator.state.StateFile;

public class FileCreatorCommand extends AbstractCommand {

  private Logger logger = Logger.getLogger(getClass().getName());
  private static final Random r = new Random();
  private int fileId = 0;
  
  public FileCreatorCommand(IExecutor executor) {
    super(executor);
  }
  
  public FileCreatorCommand(IExecutor executor, int fileId) {
    super(executor);
    this.fileId = fileId;
  }

  @Override
  public void execute() {
    StateFile sfile = null;
    try {
      do {
        sfile = newStateFile();
        if(sfile != null) {
          // 1. check file id is unique
          ResultSet rs = (ResultSet) this.executor.run(ExecuteType.FINDFILEID, sfile);
          if(!rs.next()) {
            // 2. check file pid name name
            rs = (ResultSet) this.executor.run(ExecuteType.FINDPIDNAME, sfile);
            if(!rs.next()) {
              // 3. insert file ok
              this.executor.run(ExecuteType.CREATEFILE, sfile);
              break;
            } else {
              logger.error("duplicated file name: " + sfile);
              break;
            }
          } else {
            fileId = 0; // reset file id
          }
        }
      } while(true);
    } catch (Exception e) {
      e.printStackTrace();
    }  
  }
  
  private StateFile newStateFile() {
    if(fileId == 0) {
      do {
        fileId = r.nextInt();
        if(fileId != 0) {
          break;
        }
      } while(true);
    }
    return StateFile.createStateFile(fileId);
  }

}
