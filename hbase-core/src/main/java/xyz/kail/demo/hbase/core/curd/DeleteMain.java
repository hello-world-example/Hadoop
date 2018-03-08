package xyz.kail.demo.hbase.core.curd;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import xyz.kail.demo.hbase.core.util.HBaseUtils;

/**
 * TODO 注释
 *
 * @author kaibin.yang@ttpai.cn
 */
public class DeleteMain {

    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtils.getConnection();

        Table table = connection.getTable(TableName.valueOf("_kail_test_remark"));

        Delete delete = new Delete(Bytes.toBytes("123_rowkey"));
        delete.addColumn(Bytes.toBytes("remark"), Bytes.toBytes("200"), 1L);
        table.delete(delete);


        HBaseUtils.close(connection);

    }

}
