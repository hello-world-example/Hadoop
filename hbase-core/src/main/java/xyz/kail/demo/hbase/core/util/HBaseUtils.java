package xyz.kail.demo.hbase.core.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * TODO 注释
 *
 * @author kaibin.yang@ttpai.cn
 */
public class HBaseUtils {

    public static Connection getConnection() {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "s02.hadoop.ttp.wx:2181,s03.hadoop.ttp.wx:2181,s04.hadoop.ttp.wx:2181");

        try {
            return ConnectionFactory.createConnection(hbaseConf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static void close(Closeable closeable) {
        try {
            closeable.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void printHTableDescriptor(HTableDescriptor hTableDescriptor) {
        TableName tableName = hTableDescriptor.getTableName();
        System.out.println("tableName:" + tableName);

        Map<String, String> configuration = hTableDescriptor.getConfiguration();
        Set<Map.Entry<String, String>> entries = configuration.entrySet();
        System.out.println("    configuration:");
        for (Map.Entry<String, String> entry : entries) {
            System.out.println("        " + entry.getKey() + ":" + entry.getValue());
        }


        System.out.println("    ColumnFamilies:");
        HColumnDescriptor[] columnFamilies = hTableDescriptor.getColumnFamilies();
        for (HColumnDescriptor columnDescriptor : columnFamilies) {
            System.out.println("        " + columnDescriptor);
        }
        System.out.println();
        System.out.println();
    }

}
