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

package com.taobao.adfs.iosimulator.state;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;


public class StateBlock extends StateTable{
  
  public static final String[] columns = { "id", "datanodeId", "numbytes", "generationStamp",
    "fileId", "fileIndex", "version", "operateIdentifier" };

  private long id; // blockId
  private int datanodeId;
  private long numbytes;
  private long genstamp;
  private int fileId;
  private int fileIndex;

  
  public static StateBlock createStateBlock(long id, int dnId) {
    return new StateBlock(id, dnId, 0, id, 0, 0);
  }
  
  public static StateBlock createStateBlock(long id, int dnId, 
      long genstamp, int fileId, int fileIndex) {
    return new StateBlock(id, dnId, 0, genstamp, fileId, fileIndex);
  }
  
  private StateBlock(long id, int dnId, long numbytes, 
      long genstamp, int fileId, int fileIndex) {
    this.id = id;
    this.datanodeId = dnId;
    this.numbytes = numbytes;
    this.genstamp = genstamp;
    this.fileId = fileId;
    this.fileIndex = fileIndex;
    this.version = id;
  }
  
  public void loadStateBlock(ResultSet rs) {
    try {
      id = Integer.parseInt(rs.getString(1));
      datanodeId = Integer.parseInt(rs.getString(2));
      numbytes = Long.parseLong(rs.getString(3));
      genstamp = Long.parseLong(rs.getString(4));
      fileId = Integer.parseInt(rs.getString(5));
      fileIndex = Integer.parseInt(rs.getString(6));
      version = Long.parseLong(rs.getString(7));
    } catch(Exception e) {}
  }
  
  public void updateNumBytes(long newlen) {
    numbytes = newlen;
  }
  
  public void updateDatanode(int dnId) {
    datanodeId = dnId;
  }
  
  public void updateGenStamp(long gs) {
    genstamp = gs;
  }

  @Override
  public String[] getFields(ExecuteType type) {
    return columns;
  }

  @Override
  public String[] toExecutable(ExecuteType type) {
    switch(type) {
    case CREATEBLOCK:
      return strarrayCreateBlock();
    case FINDBLOCKID:
      return strarrayFindBlock();
    case FINDBLOCKBYFILE:
      return strarrayFindBlockByFile();
    case FINDBLOCKBYDN:
      return strarrayFindBlockByDatanode();
    case UPDATEBLOCK:
      return strarrayUpdateBlock();
    case DELETEBLOCK:
      return strarrayDeleteBlock();
    }
    return null;
  }
  
  private String[] strarrayCreateBlock() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(id));
    args.add(String.valueOf(datanodeId));
    args.add(String.valueOf(numbytes));
    args.add(String.valueOf(genstamp));
    args.add(String.valueOf(fileId));
    args.add(String.valueOf(fileIndex));
    args.add(String.valueOf(version));
    args.add(operateIdentifier);
    return args.toArray(new String[0]);
  }
  
  private String[] strarrayFindBlock() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(id));
    return args.toArray(new String[0]);
  }
  
  private String[] strarrayFindBlockByFile() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(fileId));
    return args.toArray(new String[0]);
  }
  
  private String[] strarrayFindBlockByDatanode() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(datanodeId));
    return args.toArray(new String[0]);
  }
  
  private String[] strarrayUpdateBlock() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(id));
    args.add(String.valueOf(datanodeId));
    args.add(String.valueOf(numbytes));
    args.add(String.valueOf(genstamp));
    args.add(String.valueOf(fileId));
    args.add(String.valueOf(fileIndex));
    args.add(String.valueOf(version));
    args.add(operateIdentifier);
    return args.toArray(new String[0]);    
  }
  
  private String[] strarrayDeleteBlock() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(id));
    return args.toArray(new String[0]);
  }
  
  public int getFileId() {
    return fileId;
  }
  
  public long getNumbytes() {
    return numbytes;
  }
  
  public String toString() {
    return String.format("[id=%d, dnid=%d, fid=%d, fidx=%d]", id, datanodeId, fileId, fileIndex);
  }

  @Override
  public String[] getKeys(ExecuteType type) {
    return null;
  }

}
