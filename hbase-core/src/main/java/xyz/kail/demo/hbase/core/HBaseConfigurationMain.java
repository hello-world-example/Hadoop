package xyz.kail.demo.hbase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by kail on 2018/3/5.
 */
public class HBaseConfigurationMain {

    public static void main(String[] args) {
        //创建HBase的配置对象
        Configuration hbaseConf = HBaseConfiguration.create();
        Iterator<Map.Entry<String, String>> iterator = hbaseConf.iterator();
        for (; iterator.hasNext(); ) {
            Map.Entry<String, String> next = iterator.next();
            System.out.println(next.getKey() + ":" + next.getValue());
        }
    }

}
