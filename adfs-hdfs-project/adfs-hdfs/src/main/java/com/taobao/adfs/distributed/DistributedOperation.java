package com.taobao.adfs.distributed;

import java.util.Arrays;

import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Column;
import com.taobao.adfs.distributed.rpc.RPC.Invocation;
import com.taobao.adfs.util.IpAddress;
import com.taobao.adfs.util.Utilities;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
public class DistributedOperation {
  volatile boolean written = false;
  protected DistributedOperator operator = null;
  protected DistributedOperand operand = null;

  public DistributedOperation() {
  }

  public DistributedOperation(DistributedOperator operator, DistributedOperand operand) {
    assert operator != null : "operator cannot be null";
    assert operand != null : "operand cannot be null";
    this.operator = operator;
    this.operand = operand;
  }

  public DistributedOperator getOperator() {
    return operator;
  }

  public DistributedOperand getOperand() {
    return operand;
  }

  public boolean setWritten(boolean written) {
    return this.written = written;
  }

  public String toString() {
    return String.valueOf(operator) + ":" + String.valueOf(operand) + " id:" + Arrays.toString(operand.getKey());
  }

  public OperandKey getKey() {
    return new OperandKey(operand.getClass(), operand.getKey());
  }

  @Override
  public int hashCode() {
    int hash = 0;
    Object[] keys = this.operand.getKey();
    long oneKey;
    if (Long.class.isInstance(keys[0])) {
      for (int i = 0; i < keys.length; i++) {
        oneKey = Long.valueOf(keys[i].toString());
        hash = (int) (hash * 31 + oneKey);
      }
    } else if (String.class.isInstance(keys[0])) {
      for (int i = 0; i < keys.length; i++) {
        oneKey = keys[0].hashCode();
        hash = (int) (hash + oneKey);
      }
    }
    return hash;
  }

  static public String toString(DistributedOperation[] distributedOperations) {
    if (distributedOperations == null) return "null";
    else {
      StringBuilder stringBuilder = new StringBuilder(1024);
      stringBuilder.append(DistributedOperation.class.getSimpleName());
      stringBuilder.append("[").append(distributedOperations.length).append("]={");
      for (DistributedOperation distributedOperation : distributedOperations) {
        stringBuilder.append(distributedOperation).append(',');
      }
      if (distributedOperations.length != 0) stringBuilder.append("}");
      else stringBuilder.replace(stringBuilder.length() - 1, stringBuilder.length(), "}");
      return stringBuilder.toString();
    }
  }

  /**
   * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
   */
  public enum DistributedOperator {
    INSERT, UPDATE, DELETE;
  }

  /**
   * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
   */
  static public class DistributedOperand {
    @Column()
    public int clientIp = 0;

    @Column()
    public int clientProcessId = 0;

    @Column()
    public long clientThreadId = 0;

    @Column()
    public long clientSequenceNumber = 0;

    public Object[] getKey() {
      return null;
    }

    public void setIdentifier() {
      Invocation currentInvocation = DistributedServer.getCurrentInvocation();
      clientIp = currentInvocation.getCallerAddress();
      clientProcessId = currentInvocation.getCallerProcessId();
      clientThreadId = currentInvocation.getCallerThreadId();
      clientSequenceNumber = currentInvocation.getCallerSequenceNumber();
    }

    public boolean isIdentifierMatched() {
      Invocation currentInvocation = DistributedServer.getCurrentInvocation();
      if (currentInvocation == null) return false;
      return clientIp == currentInvocation.getCallerAddress()
          && clientProcessId == currentInvocation.getCallerProcessId()
          && clientThreadId == currentInvocation.getCallerThreadId()
          && clientSequenceNumber == currentInvocation.getCallerSequenceNumber();
    }

    public void clone(DistributedOperand operand) {
      if (operand == null) return;
      clientIp = operand.clientIp;
      clientProcessId = operand.clientProcessId;
      clientThreadId = operand.clientThreadId;
      clientSequenceNumber = operand.clientSequenceNumber;
    }

    public String getClientIpString() {
      return IpAddress.getAddress(clientIp);
    }

    @Override
    public String toString() {
      return Utilities.deepToString(this);
    }
  }

  static public class OperandKey {
    private Class<?> clazz;
    private Object[] key;

    public OperandKey(Class<?> clazz, Object[] key) {
      this.clazz = clazz;
      this.key = key;
    }

    public Class<?> getClazz() {
      return clazz;
    }

    public void setClazz(Class<?> clazz) {
      this.clazz = clazz;
    }

    public Object[] getKey() {
      return key;
    }

    public void setKey(Object[] key) {
      this.key = key;
    }

    @Override
    public int hashCode() {
      int result = 0;
      result += 31 * clazz.hashCode();
      return 31 * result + Arrays.hashCode(key);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) return true;
      if (obj instanceof OperandKey) {
        OperandKey oprand = (OperandKey) obj;
        if (!clazz.equals(((OperandKey) obj).getClazz())) return false;
        Object[] otherKey = oprand.getKey();
        if (key.length != otherKey.length) return false;
        for (int i = 0, len = key.length; i < len; i++) {
          if (!key[i].equals(otherKey[i])) return false;
        }
        return true;
      }
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("class: ");
      sb.append(clazz.getName());
      sb.append(Arrays.toString(key));
      return sb.toString();
    }
  }
}
