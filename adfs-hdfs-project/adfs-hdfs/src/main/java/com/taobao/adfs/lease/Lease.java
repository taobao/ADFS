package com.taobao.adfs.lease;

import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Column;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Database;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.DistributedDataRepositoryRow;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Index;
import com.taobao.adfs.distributed.DistributedDataRepositoryBaseOnTable.Table;

/**
 * @author <a href=mailto:zhangwei.yangjie@gmail.com/jiwan@taobao.com>zhangwei/jiwan</a>
 */
@Database(name = "nn_state")
@Table()
public class Lease extends DistributedDataRepositoryRow {
  @Column(width = 255, nullable = false, binary = true, indexes = { @Index(name = "PRIMARY") })
  public String holder = null;
  @Column(nullable = false, indexes = { @Index(name = "TIME") })
  public long time = 0;

  public Lease() {
  }

  public Lease(String holder, long time, long version) {
    this.holder = holder;
    this.time = time;
    this.version = version;
  }

  public Lease(String holder, long time) {
    this.holder = holder;
    this.time = time;
  }

  static final public int HOLDER = 0x1;
  static final public int TIME = HOLDER << 1;

  @Override
  public Lease update(DistributedDataRepositoryRow row, int fieldsIndication) {
    if (row == null || !(row instanceof Lease)) return this;
    Lease lease = (Lease) row;
    if ((fieldsIndication & HOLDER) == HOLDER) this.holder = lease.holder;
    if ((fieldsIndication & TIME) == TIME) this.time = lease.time;
    return this;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((holder == null) ? 0 : holder.hashCode());
    result = prime * result + (int) (time ^ (time >>> 32));
    result = prime * result + (int) (version ^ (version >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Lease other = (Lease) obj;
    if (holder == null) {
      if (other.holder != null) return false;
    } else if (!holder.equals(other.holder)) return false;
    if (time != other.time) return false;
    if (version != other.version) return false;
    return true;
  }
}