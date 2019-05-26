package org.bigdata.storage

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.CompareFilter
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

class HbaseStorage(tab: String, colFam: String, col: String) extends Storage{

  private val tableName = Bytes.toBytes(tab)
  private val columnFamilyName = Bytes.toBytes(colFam)
  private val columnName = Bytes.toBytes(col)

  protected lazy val logger: Logger = Logger.getLogger(getClass)

  private lazy val HbaseConnection: Connection = {
    val configuration = HBaseConfiguration.create()
    ConnectionFactory.createConnection(configuration)
  }

  private lazy val hbaseAdmin = HbaseConnection.getAdmin()

  private lazy val htable = new HTableDescriptor(TableName.valueOf(tableName))

  private lazy val table = HbaseConnection.getTable(TableName.valueOf(tableName))

  private def createTable(): Unit = {
    htable.addFamily(new HColumnDescriptor(columnFamilyName))
    hbaseAdmin.createTable(htable)
    logger.info(s"Table $tab was created")
  }

  override def update(key: String, record: String): Unit = {
    logger.info(s"Update record with key: $key")

    val rowKey = Bytes.toBytes(key)
    val rowValue = Bytes.toBytes(record)
    val mutation = new RowMutations(rowKey)

    if(!hbaseAdmin.tableExists(TableName.valueOf(tableName))){
      logger.warn(s"Table $tab does not exists. Creating table...")
      createTable()
    }

    val put = new Put(rowKey)
    put.addColumn(columnFamilyName, columnName, rowValue)
    mutation.add(put)
    table.checkAndMutate(rowKey, columnFamilyName, columnName, CompareFilter.CompareOp.EQUAL, null, mutation)

    logger.info(s"Row $key was updated.")

  }

  override def close(): Unit = {
    table.close()
    HbaseConnection.close()
  }

}
