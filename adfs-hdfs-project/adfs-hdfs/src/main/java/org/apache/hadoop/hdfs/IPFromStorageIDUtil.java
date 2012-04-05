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

 package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.regex.*;

import com.taobao.adfs.util.IpAddress;

public class IPFromStorageIDUtil {
  
  private static Pattern p = Pattern.compile("DS-\\d+-([^-]+)-");
  
  public static String getIPStringFromStorageID(String storageID) {
    Matcher m = p.matcher(storageID);
    if(m.find()){
      return m.group(1);
    }
    return null;
  }
  
  public static int getIDFromStorageID(String storageID) {
    String ipStr = getIPStringFromStorageID(storageID);
    if(ipStr != null) {
      return getIDFromIP(ipStr);
    } else {
      return -1;
    }
  }
  
  public static int getIDFromIP(String ipaddress) {
    try {
	    return IpAddress.getAddress(ipaddress);
    } catch(IOException e) {
      e.printStackTrace();
      return -1;
    }
  }
  
  public static String getIPFromID(int id) {
    return IpAddress.getAddress(id);
  }
  
  public static void main(String[] args) {
    String[] testStorageIDs = {
        "DS-306311929-127.0.0.4-40010-1317004447404",
        "DS-81135460-127.0.0.5-40010-1317004447408",
        "DS-466107855-127.0.0.6-40010-1317004447410",
        "DS-1837274729-127.0.0.7-40010-1317004447405",
        "DS-1203863437-127.0.0.8-40010-1317004452367",
        "DS-827076779-127.0.0.9-40010-1317004447410",
        "DS-636212248-127.0.0.10-40110-1317005438784",
        "DS-215382994-unknownIP-40110-1317005438784"
    };
    
    for (String str : testStorageIDs) {
      String ip = getIPStringFromStorageID(str);
      if(ip != null) {
        System.out.println("StorageID : " + str + "\t" + "IP : " + ip);
      } else {
        System.out.println("StorageID : " + str + " cant't get the IP Address");
      }
    }
    
    System.out.println();
    
    for (String str : testStorageIDs) {
      int id = getIDFromStorageID(str);
      if(id >= 0) {
        System.out.println("StorageID : " + str + "\t" + "IP : " + id);
      } else {
        System.out.println("StorageID : " + str + " cant't get the IP Address");
      }
    }
  }

}
