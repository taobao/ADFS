package com.taobao.adfs.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.adfs.distributed.DistributedOperation.OperandKey;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedOperationQueue {
  public static final Logger logger = LoggerFactory.getLogger(DistributedOperationQueue.class);
  Map<OperandKey, ArrayList<DistributedOperation>> operationMapByKey =
      new HashMap<OperandKey, ArrayList<DistributedOperation>>();
  Map<Long, ArrayList<DistributedOperation>> operationByThreadId = new HashMap<Long, ArrayList<DistributedOperation>>();
  Set<OperandKey> operationLock = new HashSet<OperandKey>();
  volatile boolean allowAdd = true;

  public boolean setAllowAdd(boolean allowAdd) {
    return this.allowAdd = allowAdd;
  }

  public synchronized DistributedOperation[] lockAndGetOperations(long threadId) {
    if (!allowAdd) return null;
    DistributedOperation[] operations = getOperations(threadId);
    if (operations == null || operations.length == 0) return null;
    lockBuckets(operations);
    return getNotWrittenOperations(operations);
  }

  public synchronized void deleteAndUnlockOperations(long threadId) {
    DistributedOperation[] operations = getOperations(threadId);
    markOperationsAreWritten(operations);
    deleteOperations(operations);
    unlockBuckets(operations);
  }

  public synchronized void clear() {
    operationByThreadId.clear();
    operationMapByKey.clear();
    operationLock.clear();
  }

  /**
   * get operations in queue by threadId
   */
  public synchronized void add(DistributedOperation distributedOperation) {
    if (!allowAdd) return;
    // add to map(operation's key, operations)
    ArrayList<DistributedOperation> operationListByKey = operationMapByKey.get(distributedOperation.getKey());
    if (operationListByKey == null) {
      operationListByKey = new ArrayList<DistributedOperation>();
      operationMapByKey.put(distributedOperation.getKey(), operationListByKey);
    }
    operationListByKey.add(distributedOperation);

    // add to map(threadId, operations)
    ArrayList<DistributedOperation> operationListByThreadId = operationByThreadId.get(Thread.currentThread().getId());
    if (operationListByThreadId == null) {
      operationListByThreadId = new ArrayList<DistributedOperation>();
      operationByThreadId.put(Thread.currentThread().getId(), operationListByThreadId);
    }
    operationListByThreadId.add(distributedOperation);
  }

  /**
   * get operations in queue by threadId
   */
  DistributedOperation[] getOperations(long threadId) {
    ArrayList<DistributedOperation> operationListByThreadId = operationByThreadId.get(threadId);
    if (operationListByThreadId == null) return null;
    int index = -1;
    DistributedOperation operation = null;
    ArrayList<DistributedOperation> operationListByKey = null;
    Map<OperandKey, Integer> counter = new HashMap<OperandKey, Integer>();
    Integer last = -1;
    for (int i = 0, len = operationListByThreadId.size(); i < len; i++) {
      operation = operationListByThreadId.get(i);
      operationListByKey = operationMapByKey.get(operation.getKey());
      index = operationListByKey.indexOf(operation);
      last = counter.get(operation.getKey());
      if (last == null || index > last) counter.put(operation.getKey(), index);
    }
    ArrayList<DistributedOperation> result = new ArrayList<DistributedOperation>();
    for (Map.Entry<OperandKey, Integer> entry : counter.entrySet()) {
      result.addAll(operationMapByKey.get(entry.getKey()).subList(0, entry.getValue() + 1));
    }
    return result.toArray(new DistributedOperation[result.size()]);
  }

  /**
   * lock operation-buckets by operations
   */
  private void lockBuckets(DistributedOperation... operations) {
    Set<OperandKey> keySet = new HashSet<OperandKey>();
    for (DistributedOperation operation : operations) {
      keySet.add(operation.getKey());
    }
    //first detect all lock required avaliable
    for (OperandKey operationKey : keySet) {
      try {
        while (operationLock.contains(operationKey)) {
          wait();
        }
      } catch (Throwable t) {
        Utilities.logDebug(logger, t);
        throw new RuntimeException(t);
      }
    }
    // then lock all operation one time to avoid deadlock
    for (OperandKey operationKey : keySet) {
      operationLock.add(operationKey);
    }
  }

  /**
   * get not written operations, ignore those behind operations if ignoreBehind is true
   */
  private DistributedOperation[] getNotWrittenOperations(DistributedOperation... operations) {
    if (operations == null) return null;
    List<DistributedOperation> operationList = new ArrayList<DistributedOperation>();
    for (DistributedOperation operation : operations) {
      if (!operation.written) operationList.add(operation);
    }
    return operationList.toArray(new DistributedOperation[operationList.size()]);
  }

  /**
   * delete operations in queue
   */
  private void deleteOperations(DistributedOperation... operations) {
    operationByThreadId.remove(Thread.currentThread().getId());
    if (operations == null) return;
    List<DistributedOperation> operationList = null;
    // delete from operationByKey
    for (DistributedOperation operation : operations) {
      operationList = operationMapByKey.get(operation.getKey());
      operationList.remove(operation);
      if (operationList.isEmpty()) operationMapByKey.remove(operation.getKey());
    }
  }

  /**
   * mark operations are written
   */
  private void markOperationsAreWritten(DistributedOperation... operations) {
    if (operations == null) return;
    for (DistributedOperation operation : operations) {
      if (operation == null) continue;
      operation.setWritten(true);
    }
  }

  /**
   * unlock operation-buckets by operations
   */
  void unlockBuckets(DistributedOperation... operations) {
    if (operations == null) return;
    for (DistributedOperation operation : operations) {
      operationLock.remove(operation.getKey());
    }
    notifyAll();
  }
}
