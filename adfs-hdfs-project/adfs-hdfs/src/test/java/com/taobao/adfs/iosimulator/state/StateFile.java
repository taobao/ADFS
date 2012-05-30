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

package com.taobao.adfs.iosimulator.state;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class StateFile extends StateTable{
  
  private static final int OWNER = 51199;
  public static final String[] columns = { "id", "parentId", "name", "length",
    "blockSize", "replication", "atime", "mtime", "owner", "version", "operateIdentifier" };
  
  private int id;  // file ID
  private int parentId; // parent ID
  private String name; // name
  private long length; // length
  private int blocksize = 64*1024*1024;
  private int replication = 3;
  private long atime;
  private long mtime;
  private int owner = OWNER;
  
  public static StateFile createStateFile(int id) {
    return new StateFile(id, 0, "file" + id);
  }
  
  private StateFile(int id, int parentid, String name) {
    this.id = id;
    this.parentId = parentid;
    this.name = name;
    this.length = 0;
    long now = System.currentTimeMillis();
    this.atime = now;
    this.mtime = now;
    this.version = id;
  }
  
  public void loadStateFile(ResultSet rs) {
      try {
        id = Integer.parseInt(rs.getString(1));
        parentId = Integer.parseInt(rs.getString(2));
        name = rs.getString(3);
        length = Long.parseLong(rs.getString(4));
        atime = Long.parseLong(rs.getString(5));
        mtime = Long.parseLong(rs.getString(6));
        version = Long.parseLong(rs.getString(7));
      } catch(Exception e) {}
  }
  
  public void updateLength(long newlen) {
    if(newlen < 0) {
      newlen += 10;
    }
    length = newlen;
  }
  
  public void updateRepl(int newrep) {
    replication = newrep;
  }
  
  public void updateName() {
    name = "r" + name;
  }
  
  @Override
  public String[] getFields(ExecuteType type) {
    switch(type) {
    case UPDATEFILE:
      return new String[] {"id", "parentId", 
          "name", "length", "replication", "atime", "mtime"};
    }
    return columns;
  }

  @Override
  public String[] toExecutable(ExecuteType type) {
   
    switch(type) {
    case CREATEFILE:
      return strarrayCreateFile();
    case FINDFILEID:
      return strarrayFindFileId();
    case FINDPIDNAME:
      return strarrayFindPidName();
    case UPDATEFILE:
      return strarrayUpdateFile();
    case DELETEFILE:
      return strarrayDelFileId();
    }
    return null;
  }
  
  private String[] strarrayCreateFile() {
    List<String> args = new ArrayList<String>();
    long now = System.currentTimeMillis();
    args.add(String.valueOf(id));
    args.add(String.valueOf(parentId));
    args.add(name);
    args.add(String.valueOf(length));
    args.add(String.valueOf(blocksize));
    args.add(String.valueOf(replication));
    args.add(String.valueOf(atime=now));
    args.add(String.valueOf(mtime=now));
    args.add(String.valueOf(owner));
    args.add(String.valueOf(version));
    args.add(operateIdentifier);
    return args.toArray(new String[0]);
  }
  
  private String[] strarrayFindFileId() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(id));
    return args.toArray(new String[0]);
  }
  
  private String[] strarrayFindPidName() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(parentId));
    args.add(name);
    return args.toArray(new String[0]);
  }
  
  private String[] strarrayUpdateFile() {
    List<String> args = new ArrayList<String>();
    long now = System.currentTimeMillis();
    args.add(String.valueOf(id));
    args.add(String.valueOf(parentId));
    args.add(name);
    args.add(String.valueOf(length));
    args.add(String.valueOf(replication));
    args.add(String.valueOf(atime=now));
    args.add(String.valueOf(mtime=now));
    return args.toArray(new String[0]);
  }
  
  private String[] strarrayDelFileId() {
    List<String> args = new ArrayList<String>();
    args.add(String.valueOf(id));
    return args.toArray(new String[0]);
  }
  
  public String toString() {
    return String.format("[id=%d, name=%d, pid=%d]", id, name, parentId);
  }

  @Override
  public String[] getKeys(ExecuteType type) {
    switch(type) {
    case UPDATEFILE:
      return new String[] {String.valueOf(id)};
    }
    return null;
  }
}
