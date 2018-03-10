package xyz.kail.demo.hbase.core.bulkload.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URLDecoder;

/**
 * https://www.jianshu.com/p/707cd52ca66b  Hbase BulkLoad机制
 */
public class JobBrowseCountToHBase {

    private static final String bulkPath = "/home/xxx/xxx/job_browse_bulkload";

    public static class JobBrowseMapper extends Mapper<Object, Text, Text, IntWritable> {

        public static String fileName = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
/*        if(context.getInputSplit() instanceof FileSplit){
            FileSplit split = (FileSplit)context.getInputSplit();
            Path filePath = split.getPath();
            fileName = filePath.getName();
            System.out.println("splitPath is "+filePath.toString()+" splitName is "+fileName);
        }*/
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String itermContent = value.toString();
            String iterms[] = itermContent.split("\t");
        /*StringBuffer buff = new StringBuffer();
        for(String it : iterms){
            buff.append(it).append("###");
        }
        System.out.println(buff.toString());*/
            if (iterms.length >= 26 && iterms[iterms.length - 1] != null && iterms[iterms.length - 1].equals("pv") && isNotBlank(iterms[iterms.length - 2])) {
                String UrlUndecode = iterms[25];
                if (UrlUndecode != null && !"".equals(UrlUndecode)) {
                    String Urldecode = URLDecoder.decode(UrlUndecode);
                    if (Urldecode != null && !"".equals(Urldecode)) {
                        if (Urldecode.matches("^(http://www.xxx.com/job/){1}[0-9a-z.]*") || Urldecode.matches("^(http://m.xxx.com/job/){1}[0-9a-z.]*")) {
                            String pathContent[] = Urldecode.split("/");
                            String itermUrl = pathContent[pathContent.length - 1];
                            if (itermUrl != null) {
                                String jobs[] = itermUrl.split("\\.");
                                String jobId = jobs[0];
                                String comId = iterms[iterms.length - 2];
                                if (isNotBlank(comId) && !comId.equals("-")) {
                                    String addtimeAll = iterms[19];
                                    if (isNotBlank(addtimeAll)) {
                                        String addtime = addtimeAll.substring(0, 8);
                                        String rowkey1 = comId + "_" + "1" + "_" + addtime + "_" + jobId;
                                        String rowkey2 = comId + "_" + "2" + "_" + jobId + "_" + addtime;
                                        context.write(new Text(rowkey1), new IntWritable(1));
                                        context.write(new Text(rowkey2), new IntWritable(1));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }


        boolean isNotBlank(String content) {
            return content != null && !"".equals(content);
        }

    }


    public static class JobBrowseReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
        }

        @Override
        protected void reduce(Text text, Iterable<IntWritable> iterator,
                              Context context) throws IOException, InterruptedException {
            if (text != null && !"".equals(text.toString())) {
                int browseCount = 0;
                for (IntWritable count : iterator) {
                    browseCount += count.get();
                }
                context.write(text, new IntWritable(browseCount));
            }
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

        }

    }


    public static class LoadJobBrowseToHBaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String content = value.toString();
            String kvContents[] = content.split("\t");
            if (kvContents.length >= 2) {
                byte[] rowkey = Bytes.toBytes(kvContents[0]);
                ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(rowkey);
                KeyValue lv = new KeyValue(rowkey, Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(kvContents[1]));

               context.write(rowKeyWritable, lv);
            }

        }
    }


    public static void main(String args[]) throws Exception {

        String tableName = "hdp_xxx:xxx_job_browse";
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <in> [<in>...] <out>");
            System.exit(2);
        }
        conf.set("mapreduce.job.queuename", "root.offline.xxx.normal");

        Job job = Job.getInstance(conf, "job_browse_to_hdfs");

        job.setJarByClass(JobBrowseCountToHBase.class);
        job.setMapperClass(JobBrowseMapper.class);
        job.setReducerClass(JobBrowseReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileSystem fs = FileSystem.get(conf);
        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.setInputPaths(job, new Path(otherArgs[i]));
        }

        if (fs.exists(new Path(otherArgs[otherArgs.length - 1]))) {
            fs.delete(new Path(otherArgs[otherArgs.length - 1]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[(otherArgs.length - 1)]));
        job.setNumReduceTasks(20);
        int flag = job.waitForCompletion(true) ? 0 : 1;


        Job LoadHbaseJob = new Job(conf, "job_browse_to_hbase");
        LoadHbaseJob.setJarByClass(JobBrowseCountToHBase.class);
        LoadHbaseJob.setMapperClass(LoadJobBrowseToHBaseMapper.class);
 /*   
    LoadHbaseJob.setMapOutputKeyClass(ImmutableBytesWritable.class);  configureIncrementalLoad中已经添加了
    LoadHbaseJob.setMapOutputValueClass(KeyValue.class);
    */
        if (fs.exists(new Path(bulkPath))) {
            fs.delete(new Path(bulkPath));//输出目录如果存在，先删除掉
        }

        FileInputFormat.addInputPath(LoadHbaseJob, new Path(otherArgs[otherArgs.length - 1]));
        FileOutputFormat.setOutputPath(LoadHbaseJob, new Path(bulkPath));

        Configuration hbaseConfiguration = HBaseConfiguration.create();
        HTable wordCountTable = new HTable(hbaseConfiguration, tableName);
        HFileOutputFormat2.configureIncrementalLoad(LoadHbaseJob, wordCountTable);
        int LoadHbaseJobFlag = LoadHbaseJob.waitForCompletion(true) ? 0 : 1;

        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConfiguration);
        loader.doBulkLoad(new Path(bulkPath), wordCountTable);

        System.out.println("Load Hbase Sucess");
        System.exit(LoadHbaseJobFlag);


    }
}

