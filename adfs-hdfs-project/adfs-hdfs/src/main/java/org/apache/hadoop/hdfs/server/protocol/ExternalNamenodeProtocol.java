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

import java.io.IOException;
import org.apache.hadoop.ipc.VersionedProtocol;

/*****************************************************************************
 * Protocol that a name node uses to communicate with the DataNode.
 * It's used by a name node to notify a data node who is not reported 
 * to the name node to execute commands
 *****************************************************************************/
public interface ExternalNamenodeProtocol extends VersionedProtocol {
  /**
   * 1: Added sendCommand.
   */
  public static final long versionID = 1L;

  /**
   * It's used by Name Node to send Data Node a command.
   * @return boolean true if the command is successfully sent, otherwise false.
   * @throws IOException
   */
  public boolean sendCommand(DatanodeCommand dncmd)
  throws IOException;
}
