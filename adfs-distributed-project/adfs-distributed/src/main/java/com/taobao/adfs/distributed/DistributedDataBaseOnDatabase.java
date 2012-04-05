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

package com.taobao.adfs.distributed;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.taobao.adfs.database.DatabaseExecutor;
import com.taobao.adfs.database.MysqlServerController;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.DistributedDataRepositoryRow;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.ReentrantReadWriteLockExtension;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
abstract public class DistributedDataBaseOnDatabase extends DistributedData {
  protected List<DistributedDataRepositoryBaseOnTable> repositories =
      new ArrayList<DistributedDataRepositoryBaseOnTable>();
  protected MysqlServerController mysql = new MysqlServerController();
  protected DatabaseExecutor databaseExecutor = null;

  public DistributedDataBaseOnDatabase() {
  }

  public DistributedDataBaseOnDatabase(ReentrantReadWriteLockExtension getDataLocker) {
    super(getDataLocker);
  }

  protected void initialize() throws IOException {
    try {
      super.open();
      // add repositories
      for (DistributedDataRepositoryBaseOnTable repository : createRepositories()) {
        if (repository != null) repositories.add(repository);
      }
      // set mysql conf
      if (DatabaseExecutor.needMysqlServer(conf)) {
        // add default mysql bin path = $installPath/tool/mysql/bin:$installPath/tool/mysql-$OsType/bin
        StringBuilder mysqlBinPath = new StringBuilder();
        mysqlBinPath.append(conf.get("mysql.server.bin.path", ""));
        String installPath = Utilities.getLibPath().getParent();
        mysqlBinPath.append(File.pathSeparator).append(installPath).append("/tool/mysql-")
            .append(Utilities.getOsType()).append("/bin");
        conf.set("mysql.server.bin.path", mysqlBinPath.toString());
        Utilities.logInfo(logger, "mysqlBinPath=", conf.get("mysql.server.bin.path"));
        // mysql settings
        conf.set("mysql.server.data.path", Utilities.getPath(conf.get("distributed.data.path"), "/mysql"));
        conf.set("mysql.server.restore", "false");
        Integer port = Utilities.getPort(conf.get("distributed.server.name"));
        if (port == null) port = 40000;
        Utilities.setConfDefaultValue(conf, MysqlServerController.mysqlConfKeyPrefix + "mysqld.port", port + 1);
        // mysql sql statement
        String sql = "";
        for (DistributedDataRepositoryBaseOnTable repository : repositories) {
          sql += repository.getSql();
        }
        conf.set("mysql.server.database.create.sql.statement", sql);
      }
      // format or start mysql server
      if (conf.getBoolean("distributed.data.format", false)) {
        Utilities.logInfo(logger, "data is formating");
        super.format();
        if (DatabaseExecutor.needMysqlServer(conf)) {
          mysql.formatData(conf);
          mysql.startServer(conf);
        }
        databaseExecutor = DatabaseExecutor.get(conf);
        for (DistributedDataRepositoryBaseOnTable repository : repositories) {
          repository.open(version, databaseExecutor);
          repository.format();
        }
        Utilities.logInfo(logger, "data is formatted");
        conf.setBoolean("distributed.data.format", false);
      } else {
        if (DatabaseExecutor.needMysqlServer(conf)) mysql.startServer(conf);
        databaseExecutor = DatabaseExecutor.get(conf);
        for (DistributedDataRepositoryBaseOnTable repository : repositories) {
          repository.open(version, databaseExecutor);
        }
        version.set(getVersionFromDatabase());
      }
      conf.setBoolean("distributed.data.initialize.result", true);
      Utilities.logInfo(logger, "succeed in opening data");
    } catch (Throwable t) {
      conf.setBoolean("distributed.data.initialize.result", false);
      Utilities.logWarn(logger, "fail to open data", t);
      setDataVersion(-1);
    }
  }

  public boolean isValid() {
    for (DistributedDataRepositoryBaseOnTable repository : repositories) {
      if (repository == null || !repository.isValid()) return false;
    }
    return true;
  }

  @Override
  synchronized public void open() throws IOException {
    try {
      super.open();
      if (DatabaseExecutor.needMysqlServer(conf) && !isValid()) {
        mysql.startServer(conf);
        Utilities.logWarn(logger, "restart mysql server again");
      }
      // create handler socket client and repositories
      if (databaseExecutor == null) {
        databaseExecutor = DatabaseExecutor.get(conf);
        // open repositories
        for (DistributedDataRepositoryBaseOnTable repository : repositories) {
          repository.open(version, databaseExecutor);
        }
        version.set(getVersionFromDatabase());
      }
    } catch (Throwable t) {
      setDataVersion(-1);
      throw new IOException(t);
    }
  }

  @Override
  synchronized public void close() throws IOException {
    for (DistributedDataRepositoryBaseOnTable repository : repositories) {
      repository.close();
    }
    if (databaseExecutor != null) {
      databaseExecutor.close();
      databaseExecutor = null;
    }
    if (DatabaseExecutor.needMysqlServer(conf)) mysql.stopServer(conf);
    super.close();
  }

  @Override
  synchronized public void backup() throws IOException {
    mysql.backupData(conf);
    super.backup();
  }

  @Override
  synchronized public Object[] format() throws IOException {
    try {
      Utilities.logInfo(logger, "data is formating");
      close();
      super.format();
      if (DatabaseExecutor.needMysqlServer(conf)) {
        mysql.formatData(conf);
        mysql.startServer(conf);
      }
      open();
      List<Object> metaList = null;
      for (DistributedDataRepositoryBaseOnTable repository : repositories) {
        if (metaList == null) metaList = repository.format();
        else metaList.addAll(repository.format());
      }
      Utilities.logInfo(logger, "data is formatted");
      return metaList == null ? new Object[0] : metaList.toArray(new Object[metaList.size()]);
    } catch (Throwable t) {
      setDataVersion(-1);
      throw new IOException(t);
    }
  }

  public Object[] formatDirectly(Object[] objects) throws IOException {
    format();
    for (Object object : objects) {
      if (object != null) {
        deleteRowByPrimaryKey(object);
        insertRowByPrimaryKey(object);
      }
    }
    return objects;
  }

  synchronized public long getVersionFromDatabase() throws IOException {
    version.set(-1);
    for (DistributedDataRepositoryBaseOnTable repository : repositories) {
      version.greaterAndSet(repository.findVersionFromData());
    }
    return version.get();
  }

  public DistributedDataRepositoryRow findRowByVersion(long version) throws IOException {
    for (DistributedDataRepositoryBaseOnTable repository : repositories) {
      DistributedDataRepositoryRow row = repository.findByVersion(version);
      if (row != null) return row;
    }
    return null;
  }

  protected DistributedDataRepositoryRow findRowByPrimaryKey(Object object) throws IOException {
    if (object == null) return null;
    DistributedDataRepositoryRow row = (DistributedDataRepositoryRow) object;
    return getRepository(row.getClass()).findByKeys(row.getKeys());
  }

  protected DistributedDataRepositoryRow insertRowByPrimaryKey(Object object) throws IOException {
    if (object == null) return null;
    DistributedDataRepositoryRow row = (DistributedDataRepositoryRow) object;
    getRepository(row.getClass()).locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
    try {
      return getRepository(row.getClass()).insertPhysically(row);
    } finally {
      getRepository(row.getClass()).locker.unlock(null, row.getKeys());
    }
  }

  protected DistributedDataRepositoryRow updateRowByPrimaryKey(Object object) throws IOException {
    if (object == null) return null;
    DistributedDataRepositoryRow row = (DistributedDataRepositoryRow) object;
    getRepository(row.getClass()).locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
    try {
      return getRepository(row.getClass()).updatePhysically(null, row);
    } finally {
      getRepository(row.getClass()).locker.unlock(null, row.getKeys());
    }
  }

  protected DistributedDataRepositoryRow deleteRowByPrimaryKey(Object object) throws IOException {
    if (object == null) return null;
    DistributedDataRepositoryRow row = (DistributedDataRepositoryRow) object;
    getRepository(row.getClass()).locker.lock(null, Long.MAX_VALUE, Long.MAX_VALUE, row.getKeys());
    try {
      return getRepository(row.getClass()).deletePhysically(row);
    } finally {
      getRepository(row.getClass()).locker.unlock(null, row.getKeys());
    }
  }

  public DistributedData getDataAll(DistributedData data, ReentrantReadWriteLockExtension.WriteLock writeLock)
      throws IOException {
    String remoteHost = Utilities.getHost((String) data.getElementToTransfer("distributed.server.name"));
    String remoteDataPath = (String) data.getElementToTransfer("distributed.data.path");
    conf.set("mysql.server.backup.host", Utilities.getHost(remoteHost));
    conf.set("mysql.server.backup.data.path", Utilities.getPath(remoteDataPath, "/mysql"));
    mysql.getData(conf, writeLock);
    return data;
  }

  public void setDataAll(DistributedData data) throws IOException {
    mysql.setData(conf);
  }

  public DistributedData getDataIncrement(DistributedData oldData, ReentrantReadWriteLockExtension.WriteLock writeLock)
      throws IOException {
    long versionFrom = (Long) oldData.getElementToTransfer("distributed.data.restore.increment.version.from");
    long versionTo = (Long) oldData.getElementToTransfer("distributed.data.restore.increment.version.to");
    if (versionTo - versionFrom + 1 > Integer.MAX_VALUE) throw new IOException("version gap is too large");
    int versionGap = (int) (versionTo - versionFrom + 1);

    // get an array to store increment data by version and primary key
    Object[] incrementByVersion =
        (Object[]) oldData.getElementToTransfer("distributed.data.restore.increment.by.version");
    if (incrementByVersion == null) {
      incrementByVersion = new Object[versionGap];
      oldData.putElementToTransfer("distributed.data.restore.increment.by.version", incrementByVersion);
    } else {
      if (incrementByVersion.length != versionGap) throw new IOException("inconsistent size of increment data");
      oldData.putElementToTransfer("distributed.data.restore.increment.by.primary.key", new Object[versionGap]);
    }

    Object[] incrementByPrimaryKey =
        (Object[]) oldData.getElementToTransfer("distributed.data.restore.increment.by.primary.key");
    // get increment data by version and id and save to an array
    for (int i = 0; i < incrementByVersion.length; ++i) {
      if (incrementByPrimaryKey != null && incrementByVersion[i] != null)
        incrementByPrimaryKey[i] = findRowByPrimaryKey(incrementByVersion[i]);
      incrementByVersion[i] = findRowByVersion(versionFrom + i);
    }

    return oldData;
  }

  public void setDataIncrement(DistributedData data) throws IOException {
    Object[] incrementFromSelfByVersion =
        (Object[]) getElementToTransfer("distributed.data.restore.increment.by.version");
    Object[] incrementFromMasterByVersion =
        (Object[]) data.getElementToTransfer("distributed.data.restore.increment.by.version");
    Object[] incrementFromMasterByPrimaryKey =
        (Object[]) data.getElementToTransfer("distributed.data.restore.increment.by.primary.key");
    if (incrementFromSelfByVersion.length != incrementFromMasterByVersion.length)
      throw new IOException("inconsistent size of increment data");

    // restore
    for (int i = 0; i < incrementFromSelfByVersion.length; ++i) {
      if (incrementFromSelfByVersion[i] == null && incrementFromMasterByVersion[i] == null) {
        // nothing to do
      } else if (incrementFromSelfByVersion[i] == null && incrementFromMasterByVersion[i] != null) {
        if (findRowByPrimaryKey(incrementFromMasterByVersion[i]) == null) insertRowByPrimaryKey(incrementFromMasterByVersion[i]);
        else updateRowByPrimaryKey(incrementFromMasterByVersion[i]);
      } else if (incrementFromSelfByVersion[i] != null && incrementFromMasterByVersion[i] == null) {
        if (incrementFromMasterByPrimaryKey[i] == null) deleteRowByPrimaryKey(incrementFromSelfByVersion[i]);
        else updateRowByPrimaryKey(incrementFromMasterByPrimaryKey[i]);
      } else {
        if (incrementFromMasterByPrimaryKey[i] == null) deleteRowByPrimaryKey(incrementFromSelfByVersion[i]);
        else updateRowByPrimaryKey(incrementFromMasterByPrimaryKey[i]);
        if (findRowByPrimaryKey(incrementFromMasterByVersion[i]) == null) insertRowByPrimaryKey(incrementFromMasterByVersion[i]);
        else updateRowByPrimaryKey(incrementFromMasterByVersion[i]);
      }
    }

    removeElementToTransfer("distributed.data.restore.increment.by.version");
  }

  abstract protected List<DistributedDataRepositoryBaseOnTable> createRepositories() throws IOException;

  DistributedDataRepositoryBaseOnTable getRepository(Class<?> rowClass) throws IOException {
    if (rowClass != null) {
      for (DistributedDataRepositoryBaseOnTable reopsitory : repositories) {
        if (rowClass.equals(reopsitory.getRowClass())) return reopsitory;
      }
    }
    throw new IOException("no repository for row class " + rowClass.getName());
  }

  @Override
  public Invocation getDirectInvocation(Invocation invocation) throws IOException {
    Invocation directInvocation = super.getDirectInvocation(invocation);
    if (directInvocation != null) return directInvocation;
    if (invocation.getMethod().getReturnType().getComponentType() == null)
      throw new IOException("return type should be Array for " + invocation);
    String methodName = invocation.getMethodName().substring(0, 6) + "Directly";
    Class<?>[] newMethodParmeterClasses = new Class<?>[] { Object[].class };
    return new Invocation(this, methodName, newMethodParmeterClasses, invocation.getResult());
  }

  public Object insertDirectly(Object[] rowArray) throws IOException {
    for (int i = 0; i < rowArray.length; ++i) {
      Object row = rowArray[i];
      if (row == null || !DistributedDataRepositoryRow.class.isAssignableFrom(row.getClass())) continue;
      rowArray[i] = getRepository(rowArray[i].getClass()).insertDirectly((DistributedDataRepositoryRow) row);
    }
    return rowArray;
  }

  public Object updateDirectly(Object[] rowArray) throws IOException {
    for (int i = 0; i < rowArray.length; ++i) {
      Object row = rowArray[i];
      if (row == null || !DistributedDataRepositoryRow.class.isAssignableFrom(row.getClass())) continue;
      rowArray[i] = getRepository(row.getClass()).updateDirectly((DistributedDataRepositoryRow) row);
    }
    return rowArray;
  }

  public Object deleteDirectly(Object[] rowArray) throws IOException {
    for (int i = 0; i < rowArray.length; ++i) {
      Object row = rowArray[i];
      if (row == null || !DistributedDataRepositoryRow.class.isAssignableFrom(row.getClass())) continue;
      rowArray[i] = getRepository(row.getClass()).deleteDirectly((DistributedDataRepositoryRow) row);
    }
    return rowArray;
  }

  public DistributedLocker[] getDataRepositoryLockers() {
    List<DistributedLocker> lockerList = new ArrayList<DistributedLocker>();
    for (DistributedDataRepositoryBaseOnTable repository : repositories) {
      if (repository != null) lockerList.add(repository.locker);
    }
    return lockerList.toArray(new DistributedLocker[lockerList.size()]);
  }
}
