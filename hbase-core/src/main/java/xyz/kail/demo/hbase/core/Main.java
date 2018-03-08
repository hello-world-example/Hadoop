package xyz.kail.demo.hbase.core;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Created by kail on 2018/3/5.
 */
public class Main {

    public static void main(String[] args) throws IOException {
        //创建HBase的配置对象
        Configuration hbaseConf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(hbaseConf);

        Admin admin = connection.getAdmin();

        Table htable = connection.getTable(TableName.valueOf("test_hello"));



    }

}
