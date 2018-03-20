package xyz.kail.demo.hbase.core;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter;
import org.apache.hadoop.util.ToolRunner;

import java.util.Arrays;
import java.util.List;

/**
 * Created by kail on 2018/3/5.
 */
public class Main {

    public static void main(String[] args) throws Exception {

        List<String> params = Arrays.asList(
                "-v",
                "-e",
                "-f", "/hbase/data/default/test/6ade7062cf7a8893ff685e5a8fa4d587/f/d6794bdd1094458eb71084502545d4fa"
        );

        args = params.toArray(new String[params.size()]);


        Configuration conf = HBaseConfiguration.create();

        conf.set(HConstants.HBASE_DIR, "hdfs://localhost:9000/hbase");

        int ret = ToolRunner.run(conf, new HFilePrettyPrinter(), args);
        System.exit(ret);

    }

}
