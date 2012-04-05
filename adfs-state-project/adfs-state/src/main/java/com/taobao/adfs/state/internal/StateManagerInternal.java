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

package com.taobao.adfs.state.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.taobao.adfs.block.Block;
import com.taobao.adfs.block.BlockRepository;
import com.taobao.adfs.datanode.Datanode;
import com.taobao.adfs.datanode.DatanodeRepository;
import com.taobao.adfs.distributed.DistributedDataBaseOnDatabase;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable;
import com.taobao.adfs.distributed.DistributedException;
import com.taobao.adfs.file.File;
import com.taobao.adfs.file.FileRepository;
import com.taobao.adfs.util.ReentrantReadWriteLockExtension;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class StateManagerInternal extends DistributedDataBaseOnDatabase implements StateManagerInternalProtocol {
  FileRepository fileRepository = null;
  BlockRepository blockRepository = null;
  DatanodeRepository datanodeRepository = null;
  public static boolean flagForHalfWriteTest = false;

  public StateManagerInternal(Configuration conf, ReentrantReadWriteLockExtension getDataLocker) throws IOException {
    super(getDataLocker);
    this.conf = (conf == null) ? new Configuration(false) : conf;
    initialize();
  }

  // for RPC

  public StateManagerInternal() throws IOException {
  }

  // for DistributedDataBaseOnMysql

  @Override
  synchronized protected List<DistributedDataRepositoryBaseOnTable> createRepositories() throws IOException {
    List<DistributedDataRepositoryBaseOnTable> repositories = new ArrayList<DistributedDataRepositoryBaseOnTable>();
    if (repositories.size() <= 0 || repositories.get(0) == null)
      repositories.add(0, fileRepository = new FileRepository(conf));
    if (repositories.size() <= 1 || repositories.get(1) == null)
      repositories.add(1, blockRepository = new BlockRepository(conf));
    if (repositories.size() <= 2 || repositories.get(2) == null)
      repositories.add(2, datanodeRepository = new DatanodeRepository(conf));
    return repositories;
  }

  // for StateManagerInternalProtocol

  public Object[] deleteFileAndBlockByPath(String path, boolean recursive) throws IOException {
    File[] files = deleteFileByPath(path, recursive);
    List<Object> rows = new ArrayList<Object>(files.length * 2);
    for (File file : files) {
      rows.add(file);
    }
    try {
      for (File file : files) {
        if (file.isDir()) continue;
        for (Block block : deleteBlockByFileId(file.id)) {
          rows.add(block);
        }
      }
      return rows.toArray(new Object[rows.size()]);
    } catch (Throwable t) {
      Object resultForServerInDistributedException = DistributedException.getDistributedExceptionResult(t);
      if (resultForServerInDistributedException != null) {
        for (Object rowInDistributedException : (Object[]) resultForServerInDistributedException) {
          rows.add(rowInDistributedException);
        }
      }
      throw new DistributedException(rows.toArray(new Object[rows.size()]), t);
    }
  }

  // for FileProtocol

  @Override
  public File[] insertFileByPath(String path, int blockSize, long length, byte replication, boolean overwrite)
      throws IOException {
    String[] namesInPath = Utilities.getNamesInPath(path);
    namesInPath[0] = File.ROOT.name;
    List<File> createdFileList = new ArrayList<File>();
    try {
      File parentFileInPath = File.ROOT;
      for (int i = 0; i < namesInPath.length; ++i) {
        if (flagForHalfWriteTest && i == 2) throw new IOException("half write test");
        boolean isLastFile = i == namesInPath.length - 1;
        String nameInPath = namesInPath[i];
        File fileInPath = new File();
        fileInPath.parentId = parentFileInPath.id;
        fileInPath.name = nameInPath;
        fileInPath.atime = System.currentTimeMillis();
        fileInPath.mtime = fileInPath.atime;
        fileInPath.length = isLastFile ? length : -1;
        fileInPath.blockSize = isLastFile ? blockSize : 0;
        fileInPath.replication = isLastFile ? replication : 0;
        fileInPath = fileRepository.insert(fileInPath, isLastFile ? overwrite : false);
        if (isLastFile || fileInPath.getNote() != null) {
          fileInPath.path = Utilities.getPathInName(namesInPath, i);
          createdFileList.add(fileInPath);
        }
        parentFileInPath = fileInPath;
      }
      return createdFileList.toArray(new File[createdFileList.size()]);
    } catch (Throwable t) {
      throw new DistributedException(createdFileList.toArray(new Object[createdFileList.size()]), t);
    }
  }

  @Override
  public File[] updateFileByFile(File file, int fieldsIndication) throws IOException {
    if (file == null) throw new IOException("file is null");
    if ((fieldsIndication & 0x40) == 0) file.mtime = System.currentTimeMillis();
    File newFile = fileRepository.update(file, fieldsIndication | 0x40);
    if (file.path != null && (fieldsIndication | 0x03) == 0) newFile.path = file.path;
    else newFile.path = findFileById(newFile.id).path;
    return new File[] { newFile };
  }

  @Override
  public File[] updateFileByPathAndFile(String path, File file, int fieldsIndication) throws IOException {
    if (file == null) throw new IOException("file is null");
    File oldTargetFile = findFileById(file.id);
    if (oldTargetFile != null && oldTargetFile.isOperateIdentifierMatched()) return new File[] { oldTargetFile };
    File oldFile = findFileByPath(path);
    if (oldFile == null) throw new IOException("not existed path=" + path);
    return updateFileByFile(oldFile.update(file, fieldsIndication), fieldsIndication);
  }

  @Override
  public File[] updateFileByPathAndPath(String path, String targetPath) throws IOException {
    File oldTargetFile = findFileByPath(targetPath);
    if (oldTargetFile != null && oldTargetFile.isOperateIdentifierMatched()) return new File[] { oldTargetFile };

    File file = findFileByPath(path);
    if (file == null) throw new IOException("not existed path: " + path);
    if (path.equals(targetPath)) return new File[] { file };

    List<File> files = null;
    String parentPath = new java.io.File(targetPath).getParent();
    File parentFile = findFileByPath(parentPath);
    if (parentFile == null) {
      File[] parentFiles = insertFileByPath(parentPath, 0, -1, (byte) 0, false);
      files = new ArrayList<File>(parentFiles.length + 1);
      for (int i = 0; i < parentFiles.length; ++i) {
        files.add(parentFiles[i]);
      }
    } else files = new ArrayList<File>(1);

    try {
      file.parentId = parentFile == null ? files.get(files.size() - 1).id : parentFile.id;
      file.name = new java.io.File(targetPath).getName();
      file = updateFileByPathAndFile(path, file, 0x03)[0];
      files.add(file);
      return files.toArray(new File[files.size()]);
    } catch (Throwable t) {
      throw new DistributedException(files.toArray(new Object[files.size()]), t);
    }
  }

  @Override
  public File[] deleteFileByPath(String path, boolean recursive) throws IOException {
    File file = findFileByPath(path);
    if (file == null) return new File[0];
    if (file.isDir() && !recursive && !fileRepository.findByParentId(file.id).isEmpty())
      throw new IOException("fail to delete " + path + ": find children");
    List<File> deletedFiles = new ArrayList<File>();
    try {
      if (file.isDir()) {
        for (File fileToDelete : findFileChildrenByPath(path)) {
          if (fileToDelete == null) continue;
          else if (!fileToDelete.isDir()) {
            fileToDelete = fileRepository.delete(fileToDelete);
            if (fileToDelete != null) {
              fileToDelete.path = path + "/" + fileToDelete.name;
              deletedFiles.add(fileToDelete);
            }
          } else {
            for (File deletedFile : deleteFileByPath(path + "/" + fileToDelete.name, true)) {
              deletedFiles.add(deletedFile);
            }
          }
        }
      }

      if (flagForHalfWriteTest && !deletedFiles.isEmpty()) throw new IOException("half write test");

      file = fileRepository.delete(file);
      if (file != null) {
        file.path = path;
        deletedFiles.add(file);
      }
      return deletedFiles.toArray(new File[deletedFiles.size()]);
    } catch (Throwable t) {
      Object resultForServerInDistributedException = DistributedException.getDistributedExceptionResult(t);
      if (resultForServerInDistributedException != null) {
        for (Object fileInDistributedException : (Object[]) resultForServerInDistributedException) {
          deletedFiles.add((File) fileInDistributedException);
        }
      }
      throw new DistributedException(deletedFiles.toArray(new Object[deletedFiles.size()]), t);
    }
  }

  @Override
  public File[] deleteFileByFile(File file, boolean recursive) throws IOException {
    if (file == null || (file = findFileById(file.id)) == null) return new File[0];
    return deleteFileByPath(file.path, recursive);
  }

  @Override
  public File[] deleteFileByFiles(File[] files, boolean recursive) throws IOException {
    if (files == null) return new File[0];
    List<File> deleteFiles = new ArrayList<File>();
    try {
      for (File file : files) {
        Utilities.addArrayToList(deleteFileByFile(file, recursive), deleteFiles);
        if (flagForHalfWriteTest && !deleteFiles.isEmpty()) throw new IOException("half write test");
      }
      return deleteFiles.toArray(new File[deleteFiles.size()]);
    } catch (Throwable t) {
      throw new DistributedException(deleteFiles.toArray(new Object[deleteFiles.size()]), t);
    }
  }

  @Override
  public File findFileById(int id) throws IOException {
    File file = fileRepository.findById(id);
    if (file == null) return null;
    else if (file.isRootById()) {
      file.path = "";
      return file;
    } else {
      File parentFile = findFileById(file.parentId);
      if (parentFile == null) throw new IOException("fail to get parent file for " + file);
      file.path = parentFile.path + "/" + file.name;
      return file;
    }
  }

  @Override
  public File findFileByPath(String path) throws IOException {
    if (path == null) return null;
    String[] names = Utilities.getNamesInPath(path);
    File[] files = new File[names.length];
    files[0] = fileRepository.findById(File.ROOT.id);
    for (int i = 1; i < names.length; ++i) {
      if (files[i - 1] == null) return null;
      files[i] = fileRepository.findByParentIdAndName(files[i - 1].id, names[i]);
    }
    File file = files[files.length - 1];
    if (file != null) file.path = path;
    return file;
  }

  @Override
  public File[] findFileChildrenByPath(String path) throws IOException {
    return findFileChildrenByFile(findFileByPath(path));
  }

  public File[] findFileChildrenByFile(File file) throws IOException {
    if (file == null) return null;
    else if (!file.isDir()) return new File[] { file };
    else {
      List<File> childFiles = fileRepository.findByParentId(file.id);
      for (File childFile : childFiles) {
        if (file.path.equals("/")) childFile.path = "/" + childFile.name;
        else childFile.path = file.path + "/" + childFile.name;
      }
      return childFiles.toArray(new File[childFiles.size()]);
    }
  }

  @Override
  public File[] findFileDescendantByPath(String path, boolean excludeDir, boolean includeSelfAnyway) throws IOException {
    return findFileDescendantByFile(findFileByPath(path), excludeDir, includeSelfAnyway);
  }

  public File[] findFileDescendantByFile(File file, boolean excludeDir, boolean includeSelfAnyway) throws IOException {
    List<File> files = findFileDescendantByFileInternal(file, excludeDir);
    if (excludeDir && includeSelfAnyway) files.add(0, file);
    return files == null ? null : files.toArray(new File[files.size()]);
  }

  public List<File> findFileDescendantByFileInternal(File file, boolean excludeDir) throws IOException {
    if (file == null) return null;
    List<File> files = new ArrayList<File>();
    if (!file.isDir()) {
      files.add(file);
      return files;
    }
    if (!excludeDir) files.add(file);

    File[] childFiles = findFileChildrenByFile(file);
    if (childFiles == null) return files;
    for (File childFile : childFiles) {
      if (childFile == null) continue;
      if (!file.isDir()) files.add(childFile);
      files.addAll(findFileDescendantByFileInternal(childFile, excludeDir));
    }
    return files;
  }

  // for BlockProtocol

  @Override
  public Block[] findBlockById(long blockId) throws IOException {
    return blockRepository.findById(blockId).toArray(new Block[0]);
  }

  @Override
  public Block[] findBlockByDatanodeId(int datanodeId) throws IOException {
    return blockRepository.findByDatanodeId(datanodeId).toArray(new Block[0]);
  }

  @Override
  public Block[] findBlockByFileId(int fileId) throws IOException {
    return blockRepository.findByFileId(fileId).toArray(new Block[0]);
  }

  @Override
  public Block[] findBlockByFiles(File[] files) throws IOException {
    if (files == null) return null;
    List<File> descendantFiles = new ArrayList<File>();
    for (File file : files) {
      if (file == null) continue;
      descendantFiles.addAll(findFileDescendantByFileInternal(file, false));
    }

    List<Block> blocks = new ArrayList<Block>();
    for (File file : descendantFiles) {
      if (file == null || file.isDir()) continue;
      blocks.addAll(blockRepository.findByFileId(file.id));
    }
    return blocks.toArray(new Block[blocks.size()]);
  }

  @Override
  public Block[] insertBlockByBlock(Block block) throws IOException {
    if (block == null) throw new IOException("block is null");
    return new Block[] { (Block) blockRepository.insert(block, false) };
  }

  @Override
  public Block[] updateBlockByBlock(Block block) throws IOException {
    if (block == null) throw new IOException("block is null");
    return new Block[] { (Block) blockRepository.update(block, -1) };
  }

  @Override
  public Block[] deleteBlockById(long id) throws IOException {
    List<Block> blocks = blockRepository.findById(id);
    List<Block> deletedBlocks = new ArrayList<Block>(blocks.size());
    try {
      for (Block block : blocks) {
        Block deletedBlock = (Block) blockRepository.delete(block);
        if (deletedBlock != null) deletedBlocks.add(deletedBlock);
        if (flagForHalfWriteTest && !deletedBlocks.isEmpty()) throw new IOException("half write test");
      }
      return deletedBlocks.toArray(new Block[deletedBlocks.size()]);
    } catch (Throwable t) {
      throw new DistributedException(deletedBlocks.toArray(new Object[deletedBlocks.size()]), t);
    }
  }

  @Override
  public Block[] deleteBlockByIdAndDatanodeId(long id, int datanodeId) throws IOException {
    Block block = blockRepository.findByIdAndDatanodeId(id, datanodeId);
    if (block == null) return new Block[0];
    return new Block[] { (Block) blockRepository.delete(block) };
  }

  @Override
  public Block[] deleteBlockByFileId(int fileId) throws IOException {
    List<Block> blocks = blockRepository.findByFileId(fileId);
    List<Block> deletedBlocks = new ArrayList<Block>(blocks.size());
    try {
      for (Block block : blocks) {
        Block deletedBlock = (Block) blockRepository.delete(block);
        if (deletedBlock != null) deletedBlocks.add(deletedBlock);
        if (flagForHalfWriteTest && !deletedBlocks.isEmpty()) throw new IOException("half write test");
      }
      return deletedBlocks.toArray(new Block[deletedBlocks.size()]);
    } catch (Throwable t) {
      throw new DistributedException(deletedBlocks.toArray(new Object[deletedBlocks.size()]), t);
    }
  }

  // for DatanodeProtocol

  @Override
  public Datanode[] findDatanodeById(int id) throws IOException {
    Datanode datanode = datanodeRepository.findById(id);
    return datanode == null ? new Datanode[0] : new Datanode[] { datanode };
  }

  @Override
  public Datanode[] findDatanodeByStorageId(String storageId) throws IOException {
    return datanodeRepository.findByStorageId(storageId).toArray(new Datanode[0]);
  }

  @Override
  public Datanode[] findDatanodeByUpdateTime(long updateTime) throws IOException {
    return datanodeRepository.findByUpdateTimeGreaterOrEqualThan(updateTime).toArray(new Datanode[0]);
  }

  @Override
  public Datanode[] updateDatanodeByDatanode(Datanode datanode) throws IOException {
    if (datanode == null) throw new IOException("datanode is null");
    return new Datanode[] { (Datanode) datanodeRepository.update(datanode, -1) };
  }

  @Override
  public Datanode[] insertDatanodeByDatanode(Datanode datanode) throws IOException {
    if (datanode == null) throw new IOException("datanode is null");
    return new Datanode[] { (Datanode) datanodeRepository.insert(datanode, false) };
  }
}
