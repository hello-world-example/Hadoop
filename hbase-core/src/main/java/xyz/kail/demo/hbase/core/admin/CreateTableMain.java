package xyz.kail.demo.hbase.core.admin;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.regionserver.BloomType;

import java.io.IOException;

import xyz.kail.demo.hbase.core.util.HBaseUtils;

/**
 * create '_kail_test_remark', {NAME => 'remark', VERSIONS => 100000}
 * {NAME => 'remark', DATA_BLOCK_ENCODING => 'NONE' ,BLOOMFILTER => 'ROW', REPLICATION_SCOPE => '0', VERSIONS => '100000', COMPRESSION => 'NONE', MIN_VERSIONS => '0', TTL=> 'FOREVER', KEEP_DELETED_CELLS => 'FALSE', BLOCKSIZE => '65536', IN_MEMORY => 'false', BLOCKCACHE => 'true'}
 *
 * @author kaibin.yang@ttpai.cn
 */
public class CreateTableMain {


    public static void main(String[] args) throws IOException {
        Connection connection = HBaseUtils.getConnection();
        Admin admin = connection.getAdmin();

        TableName kailTestTableName = TableName.valueOf("_kail_test_remark");



        /*
         * 如果存在要创建的表，那么先删除，再创建
         */
        if (admin.tableExists(kailTestTableName)) {
            admin.disableTable(kailTestTableName);
            admin.deleteTable(kailTestTableName);
            System.out.println(kailTestTableName.getNameAsString() + " is exist,detele....");
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(kailTestTableName);


        HColumnDescriptor remarkColumnDescriptor = new HColumnDescriptor("remark");
        remarkColumnDescriptor.setMaxVersions(Integer.MAX_VALUE);

        tableDescriptor.addFamily(remarkColumnDescriptor);


        admin.createTable(tableDescriptor);

        HBaseUtils.close(connection);
    }

}
