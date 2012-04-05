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
 
 import java.net.Socket;
 import java.net.SocketAddress;
 
 import java.util.List;
 import java.util.Map;
 
 import com.google.common.collect.LinkedListMultimap;
 import org.apache.commons.logging.Log;
 import org.apache.commons.logging.LogFactory;
 import org.apache.hadoop.io.IOUtils;
 
 /**
  * A cache of sockets.
  */
 class SocketCache {
   static final Log LOG = LogFactory.getLog(SocketCache.class);
   private static int DEFAULT_CAPACITY = 16; 
   private final LinkedListMultimap<SocketAddress, Socket> multimap;
   private int capacity;
   
   static SocketCache sktcache ;
   public static SocketCache getInstance(){
	   if(sktcache == null){
		   sktcache = new SocketCache();
	   }
	   return sktcache;
   }
 
   /**
    * Create a SocketCache with default capacity.
    */
   public SocketCache() {
     this(DEFAULT_CAPACITY);
   }
 
   /**
    * Create a SocketCache with the given capacity.
    * @param capacity  Max cache size.
    */
   public SocketCache(int capacity) {
     multimap = LinkedListMultimap.create();
     this.capacity = capacity;
   }
 
   /**
    * Get a cached socket to the given address.
    * @param remote  Remote address the socket is connected to.
    * @return  A socket with unknown state, possibly closed underneath. Or null.
    */
   public synchronized Socket get(SocketAddress remote) {
     List<Socket> socklist = multimap.get(remote);
     if (socklist == null) {
       return null;
     }
     for (Socket candidate : socklist) {
       multimap.remove(remote, candidate);
       if (!candidate.isClosed()) {
         return candidate;
       }
     }
     return null;
   }
 
   /**
    * Give an unsed socket to the cache.
    * @param sock  Socket not used by anyone.
    */
   public synchronized void put(Socket sock) {
     if (sock == null) {
       return;
     }
 
     SocketAddress remoteAddr = sock.getRemoteSocketAddress();
     if (remoteAddr == null) {
       LOG.warn("Cannot cache (unconnected) socket with no remote address: " +
                sock);
       IOUtils.closeSocket(sock);
       return;
     }
 
     if (capacity == multimap.size()) {
       evict(1);
     }
     
     multimap.put(remoteAddr, sock);

   }
 
   public int size() {
     return multimap.size();
   }
 
   /**
    * Evict entries from the cache, oldest first.
    * @param numToEvict  Number of entries to evict.
    */
   private synchronized void evict(int numToEvict) {
     int nEvicted = 0;
 
     for (Map.Entry<SocketAddress, Socket> entry: multimap.entries()) {
       Socket sock = entry.getValue();
       multimap.remove(entry.getKey(), sock);
       IOUtils.closeSocket(sock); 
       if (++nEvicted >= numToEvict) {
         break;
       }
     }
   }
 
   /**
    * Empty the cache, and close all sockets.
    */
   public synchronized void clear() {
     for (Socket sock : multimap.values()) {
       IOUtils.closeSocket(sock);
     }
     multimap.clear();
   }
 
   protected void finalize() {
     clear();
   }
 
 }
