package org.bigdata.storage

trait Storage extends Serializable {
  def update(key: String, record: String): Unit

  def close(): Unit
}
