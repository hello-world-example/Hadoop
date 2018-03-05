package xyz.kail.demo.hbase.core.admin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by kail on 2018/3/5.
 */
public class CreateTableMain {

    public static void main(String[] args) throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(hbaseConf);


        Admin admin = connection.getAdmin();


        ClusterStatus clusterStatus = admin.getClusterStatus();
        System.out.println(clusterStatus);

        int masterInfoPort = admin.getMasterInfoPort();
        System.out.println(masterInfoPort);

    }

}
