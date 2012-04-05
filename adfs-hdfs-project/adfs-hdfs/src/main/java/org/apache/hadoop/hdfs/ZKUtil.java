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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ZKUtil {
  
  private static final int sleepTime = (ZKClient.maxRetryTimes!=0)?
      (ZKClient.sessionTimeOut/ZKClient.maxRetryTimes-1):1;
      
  private static void shouldRetry(int retried) {
    if(retried >= ZKClient.maxRetryTimes) {
      throw new RuntimeException("retries of zookeeper exceed limit, aborting...");
    } else {
      try {
        TimeUnit.MILLISECONDS.sleep(sleepTime);
      } catch (InterruptedException e) {
        ZKClient.LOG.error("shouldRetry: InterruptedException", e);
      }
    }
  }
  
  static class CreateAndSetDataObj {
    int retried;
    final byte[] data;
    CreateAndSetDataObj(int retries, byte[] data) {
      this.retried = retries;
      this.data = data;
    }
  }
  
  static class CreateAsyncCallback implements AsyncCallback.StringCallback {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
      if (rc != 0) {
        if (rc == KeeperException.Code.CONNECTIONLOSS.intValue()) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (create): connection loss");
          CreateAndSetDataObj sdo = (CreateAndSetDataObj) ctx;
          shouldRetry(sdo.retried++);
          asyncCreateInternal(path, sdo);
        } else if (rc == KeeperException.Code.NODEEXISTS.intValue()) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (create): path=" + path
              + " already exists.");
        }
      } else {
        ZKClient.LOG.info("FROM ZOOKEEPER (create): " 
            + path + " is created.");
      }
    }
  }
  
  static class SetDataAsyncCallback implements AsyncCallback.StatCallback {
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat) {
      if (rc != 0) {
        if (rc == KeeperException.Code.CONNECTIONLOSS.intValue()) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (setData): connection loss");
          CreateAndSetDataObj sdo = (CreateAndSetDataObj) ctx;
          shouldRetry(sdo.retried++);
          asyncSetDataInternal(path, sdo);
        } else {
          ZKClient.LOG.error("FROM ZOOKEEPER (setData): path=" 
              + path + ", rc=" + rc);
        }
      }
    }
  }
  
  static class DeleteObj {
    int retried;
    boolean delParent;
    DeleteObj(int retries, boolean delParent) {
      this.retried = retries;
      this.delParent = delParent;
    }
  }
  
  static class DeleteAsyncCallback implements AsyncCallback.VoidCallback {
    @Override
    public void processResult(int rc, String path, Object ctx) {
      if(rc != 0) {
        if (rc == KeeperException.Code.CONNECTIONLOSS.intValue()) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (delete): connection loss");
          DeleteObj deo = (DeleteObj) ctx;
          shouldRetry(deo.retried++);
          asyncDeleteInternal(path, deo);
        } else if(rc == KeeperException.Code.NONODE.intValue()) {
          ZKClient.LOG.warn("FROM ZOOKEEPER (delete): " + 
              path + " doesn't exist.");
        } else if(rc == KeeperException.Code.NOTEMPTY.intValue()) {
            ZKClient.LOG.warn("FROM ZOOKEEPER (delete): " + 
                path + " is not empty.");
        } else {
          ZKClient.LOG.error("FROM ZOOKEEPER (delete): path=" 
              + path + ", rc=" + rc);
        }
      } else {
        ZKClient.LOG.info("FROM ZOOKEEPER (delete): " + path + " is deleted");
        DeleteObj deo = (DeleteObj) ctx;
        if(deo.delParent) {
          int lastslash = path.lastIndexOf(ZKClient.SLASH);
          String parent = path.substring(0, lastslash);
          deo.delParent = false;
          deo.retried = 0;
          asyncDeleteInternal(parent, deo);
        }
      }
    }
  }

  static void touch(String path, byte[] data, 
      boolean del) throws IOException{
    int count = 0;
    int lastslash = path.lastIndexOf(ZKClient.SLASH);
    String parent = path.substring(0, lastslash);
    String node = path.substring(lastslash+1);
    String ret = syncCreate(parent, null, false);
    long now = 0;
    if(parent.equals(ret)) {
      if(del) syncDelete(path, false);
      do {
        try {
          now=ZKClient.now();
          ZKClient.getInstance().getHandler().create(path, data, 
              Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException e) {
          if(e instanceof KeeperException.ConnectionLossException) {
            ZKClient.LOG.debug("FROM ZOOKEEPER (touch): connection loss");
            shouldRetry(count++);
            continue;
          } else {
            throw new IOException(e);
          }
        } catch (InterruptedException e) {
          throw new IOException(e);
        } finally {
          ZKClient.metrics.SyncWriteRate.inc((int)(ZKClient.now()-now));
          ZKClient.LOG.info("FROM ZOOKEEPER (touch): " + path);
        }
        break;
      } while(true);
      ZKClient.LOG.debug("FROM ZOOKEEPER (touch): the ephemeral node for " 
          + node + " is created!");
    }
  }
  
  
  static String syncCreate(String path, byte[] data, boolean parent) throws IOException {
    String retpath = null;
    String[] paths = path.split(ZKClient.SLASH);
    StringBuilder sb = new StringBuilder();
    if (!parent) {
      paths = new String[] { "", path.substring(1) };
    }
    int count = 0;
    long now = 0;
    for (int i = 1; i < paths.length; i++) {
      sb.delete(0, sb.length());
      paths[i] = sb.append(paths[i - 1]).append(ZKClient.SLASH)
          .append(paths[i]).toString();
      try {
        now=ZKClient.now();
        retpath = ZKClient.getInstance().getHandler().create(paths[i],
            ((i == paths.length - 1) ? data : new byte[0]),
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        ZKClient.LOG.info("FROM ZOOKEEPER (create): " + paths[i]
            + " is created.");
      } catch (KeeperException e) {
        if (e instanceof KeeperException.NodeExistsException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (create): " + paths[i]
              + " already exists.");
          retpath = path;
        } else if (e instanceof KeeperException.NoNodeException && !parent) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (create): " + path
              + " parents don't exists.");
          retpath = syncCreate(path, data, true);
        } else if (e instanceof KeeperException.ConnectionLossException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (create): connection loss");
          shouldRetry(count++);
          continue;
        } else {
          throw new IOException(e);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {
        ZKClient.metrics.SyncWriteRate.inc((int)(ZKClient.now()-now));
      }
    }
    return retpath;
  }
  
  static boolean syncCreate(String path) throws IOException {
    int count = 0;
    long now = 0;
    do {
      try {
        now = ZKClient.now();
        String ret = ZKClient.getInstance().getHandler().create(path, new byte[0],
            Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        if (path.equals(ret)) {
          ZKClient.LOG.info("FROM ZOOKEEPER (create): " + path);
          return true;
        }
      } catch (KeeperException e) {
        if (e instanceof KeeperException.ConnectionLossException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (create): connection loss");
          shouldRetry(count++);
          continue;
        } else if (e instanceof KeeperException.NodeExistsException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (create): node exists");
          return false;
        } else {
          throw new IOException(e);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {
        ZKClient.metrics.SyncWriteRate.inc((int)(ZKClient.now()-now));
      }
    } while (true);
  }
  
  static void asyncCreate(String path, byte[] data, 
      boolean parent) throws IOException {
    if(parent) {
      int lastslash = path.lastIndexOf(ZKClient.SLASH);
      String strParent = path.substring(0, lastslash);
      syncCreate(strParent, new byte[0], false);
    } else { // don't need to create parent
      asyncCreateInternal(path, new CreateAndSetDataObj(0, data));
    }
  }
  
  private static void asyncCreateInternal(String path, CreateAndSetDataObj obj) {
    long now=ZKClient.now();
    ZKClient.getInstance().getHandler().create(path, 
        obj.data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, 
        new CreateAsyncCallback(), obj);
    ZKClient.metrics.AsycnWriteRate.inc((int)(ZKClient.now()-now));
  }
  
  static void asyncSetData(String path, byte[] data) {
    CreateAndSetDataObj obj = new CreateAndSetDataObj(0, data);
    asyncSetDataInternal(path, obj);
  }
  
  private static void asyncSetDataInternal(String path, CreateAndSetDataObj obj) {
    long now=ZKClient.now();
    ZKClient.getInstance().getHandler().setData(path, obj.data, -1, 
        new SetDataAsyncCallback(), obj);
    ZKClient.metrics.AsycnWriteRate.inc((int)(ZKClient.now()-now));
  }
  
  static byte[] syncGetData(String path, Stat stat) throws IOException{
    byte[] ret = null;
    int count = 0;
    long now = 0;
    do {
      try {
        now=ZKClient.now();
        ret = ZKClient.getInstance().getHandler().getData(path, false, stat);
      } catch (KeeperException e) {
        if(e instanceof KeeperException.NoNodeException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (getData): No Node for path = " + path);
          ret = null;
        } else if(e instanceof KeeperException.ConnectionLossException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (getData): connection loss");
          shouldRetry(count++);
          continue;
        }else {
          throw new IOException(e);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {
        ZKClient.metrics.ReadRate.inc((int)(ZKClient.now()-now));
      }
      break;
    }while(true); 
    return ret;
  }
  
  static Stat syncExist(String path) throws IOException {
    Stat ret = null;
    int count = 0;
    long now = 0;
    do {
      try {
        now = ZKClient.now();
        ret = ZKClient.getInstance().getHandler().exists(path, false);
      } catch (KeeperException e) {
        if(e instanceof KeeperException.ConnectionLossException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (exist): connection loss");
          shouldRetry(count++);
          continue;
        } else {
          throw new IOException(e);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {
        ZKClient.metrics.ReadRate.inc((int)(ZKClient.now()-now));
      }
      break;
    }while(true);
    return ret;
  }
  
  static List<String> syncGetChildren(String path, 
      Watcher watcher) throws IOException {
    List<String> children = null;
    int count = 0;
    long now = 0;
    do {
      try {
        now = ZKClient.now();
        children = ZKClient.getInstance().getHandler().getChildren(path, watcher);
      } catch (KeeperException e) {
        if(e instanceof KeeperException.NoNodeException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (getChildren): No Node for path = " 
              + path);
          children = null;
        } else if(e instanceof KeeperException.ConnectionLossException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (getChildren): connection loss");
          shouldRetry(count++);
          continue;
        }else{
          throw new IOException(e);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {
        ZKClient.metrics.ReadRate.inc((int)(ZKClient.now()-now));
      }
      break;
    }while(true);
    return children;    
  }
  
  static boolean syncDelete(String path, boolean delParent) throws IOException {
    boolean ret = true;
    int count = 0;
    long now = 0;
    do {
      try {
        now = ZKClient.now();
        ZKClient.getInstance().getHandler().delete(path, -1);
        ZKClient.LOG.info("FROM ZOOKEEPER (delete): " + path + " is deleted.");
      } catch (KeeperException e) {
        if (e instanceof KeeperException.NoNodeException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (delete): " + path + " doesn't exist.");
          ret = false;
        }else if (e instanceof KeeperException.NotEmptyException) {
          ZKClient.LOG.warn("FROM ZOOKEEPER (delete): " + path + " is not empty.");
          ret = false;
        } else if(e instanceof KeeperException.ConnectionLossException) {
          ZKClient.LOG.debug("FROM ZOOKEEPER (delete): connection loss");
          shouldRetry(count++);
          continue;
        }else {
          throw new IOException(e);
        }
      } catch (InterruptedException e) {
        throw new IOException(e);
      } finally {
        ZKClient.metrics.SyncWriteRate.inc((int)(ZKClient.now()-now));
      }
      break;
    }while(true);
    
    if (delParent) {
      int lastslash = path.lastIndexOf(ZKClient.SLASH);
      String strparent = path.substring(0, lastslash);
      count = 0;
      do {
        try {
            now = ZKClient.now();
            ZKClient.getInstance().getHandler().delete(strparent, -1);
            ZKClient.LOG.debug("FROM ZOOKEEPER (delete): " + strparent + " is deleted.");
        } catch (KeeperException e) {
          if (e instanceof KeeperException.NoNodeException) {
            ZKClient.LOG.warn("FROM ZOOKEEPER (delete): " + strparent + " doesn't exist.");
            ret = false;
          } else if (e instanceof KeeperException.NotEmptyException) {
            ZKClient.LOG.warn("FROM ZOOKEEPER (delete): " + strparent + " is not empty.");
          } else if(e instanceof KeeperException.ConnectionLossException) {
            ZKClient.LOG.debug("FROM ZOOKEEPER (delete): connection loss");
            shouldRetry(count++);
            continue;
          }else {
            throw new IOException(e);
          }
        } catch (InterruptedException e) {
          throw new IOException(e);
        } finally {
          ZKClient.metrics.SyncWriteRate.inc(ZKClient.now()-now);
        }
        break;
      }while(true);
    }
    
    return ret;
  }
  
  static void asyncDelete(String path, boolean delParent) {
    DeleteObj obj = new DeleteObj(0, delParent);
    asyncDeleteInternal(path, obj);
  }
  
  private static void asyncDeleteInternal(String path, DeleteObj obj) {
    long now =ZKClient.now();
    ZKClient.getInstance().getHandler().delete(path, -1, 
        new DeleteAsyncCallback(), obj);
    ZKClient.metrics.AsycnWriteRate.inc(ZKClient.now()-now);
  }

}
