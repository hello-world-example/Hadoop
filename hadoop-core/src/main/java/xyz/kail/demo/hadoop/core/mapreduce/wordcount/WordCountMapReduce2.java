package xyz.kail.demo.hadoop.core.mapreduce.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import xyz.kail.demo.hadoop.core.util.MapReduceUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

public class WordCountMapReduce2 extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(WordCountMapReduce2.class);

    private static final String NEW_LINE = System.getProperty("line.separator");

    /**
     * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    public static class MyMap extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            logger.info("map key:" + key);
            logger.info("map value:" + value);


            String stuInfo = value.toString();

            //将输入的数据先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(stuInfo, NEW_LINE);

            //分别对每一行进行处理  
            while (tokenizerArticle.hasMoreTokens()) {


                String nextToken = tokenizerArticle.nextToken();
                logger.info("nextToken : " + nextToken);

                // 每行按空格划分
                StringTokenizer tokenizer = new StringTokenizer(nextToken);


                Map<String, Integer> map = new HashMap<>();
                for (; tokenizer.hasMoreTokens(); ) {
                    String word = tokenizer.nextToken();

                    if (!map.containsKey(word)) {
                        map.put(word, 0);
                    }
                    map.put(word, map.get(word) + 1);

                }

                Set<Map.Entry<String, Integer>> entries = map.entrySet();
                for (Map.Entry<String, Integer> entry : entries) {
                    logger.warn(entry.getKey());
                    logger.warn(entry.getValue().toString());
                    logger.warn("");
                    context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
                }

            }
        }

    }

    /**
     * <KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     */
    public static class MyReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));//输出学生姓名和平均值
        }

    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "wordCount");
        job.setJarByClass(WordCountMapReduce2.class);

        /*
         * 设置 Map 和 Reduce 类
         */
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);

        /*
         * 设置 Map 输出类型
         */
        job.setMapOutputKeyClass(Text.class); //
        job.setMapOutputValueClass(IntWritable.class); //

        /*
         * 设置 输入出处路径
         */
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0])); // 设置输入文件路径

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 设置输出文件路径

        /*
         * 等待执行完成
         */
        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;

    }


    public static void main(String[] args) throws Exception {

        args = MapReduceUtil.agrsIO(WordCountMapReduce2.class, args);

        int ret = ToolRunner.run(new WordCountMapReduce2(), args);
        System.exit(ret);
    }
} 