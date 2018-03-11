package xyz.kail.demo.hbase.core.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 * TODO 注释
 *
 * @author kaibin.yang@ttpai.cn
 */
public class HBaseUtils {

    public static Connection getConnection() {
        Configuration hbaseConf = HBaseConfiguration.create();
//        hbaseConf.set("hbase.zookeeper.quorum", "s02.hadoop.ttp.wx:2181,s03.hadoop.ttp.wx:2181,s04.hadoop.ttp.wx:2181");
        hbaseConf.set("hbase.zookeeper.quorum", "localhost");
        //        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");
        //        hbaseConf.set("hbase.master", "192.168.1.100:600000");
        /*
         *
         */
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

    public static void printCell(Cell cell) {
        System.out.print(Bytes.toStringBinary(CellUtil.cloneRow(cell)));
        System.out.print(":");
        System.out.print(Bytes.toStringBinary(CellUtil.cloneFamily(cell)));
        System.out.print(":");
        System.out.print(Bytes.toStringBinary(CellUtil.cloneQualifier(cell)));
        System.out.print(":");
        System.out.print(cell.getTimestamp() + "(" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date(cell.getTimestamp())) + ")");
        System.out.print(" => ");
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));

    }

}
