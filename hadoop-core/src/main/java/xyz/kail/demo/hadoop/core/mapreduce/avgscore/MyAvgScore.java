package xyz.kail.demo.hadoop.core.mapreduce.avgscore;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import java.util.StringTokenizer;

/***
 * 定义一个AvgScore 求学生的平均值 要实现一个Tool 工具类，是为了初始化一个hadoop配置实例
 *
 * [自己实现 一个MapReduce 示例](http://blog.csdn.net/liuc0317/article/details/8716368)
 */
public class MyAvgScore extends Configured implements Tool {

    private static final Logger logger = LoggerFactory.getLogger(MyAvgScore.class);

    private static final String NEW_LINE = System.getProperty("line.separator");

    /**
     * <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
     */
    public static class MyMap extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String stuInfo = value.toString();//将输入的纯文本的数据转换成String  

            logger.info("MapSudentInfo:" + stuInfo);

            //将输入的数据先按行进行分割

            StringTokenizer tokenizerArticle = new StringTokenizer(stuInfo, NEW_LINE);

            //分别对每一行进行处理  
            while (tokenizerArticle.hasMoreTokens()) {
                // 每行按空格划分
                StringTokenizer tokenizer = new StringTokenizer(tokenizerArticle.nextToken());

                String name = tokenizer.nextToken();//学生姓名
                String score = tokenizer.nextToken();//学生成绩

                Text stu = new Text(name);
                int intScore = Integer.parseInt(score);

                logger.info("MapStu:" + stu.toString() + " " + intScore);

                context.write(stu, new IntWritable(intScore)); // 输出学生姓名和成绩
            }
        }

    }

    /**
     * <KEYIN,VALUEIN,KEYOUT,VALUEOUT>
     */
    public static class MyReduce extends Reducer<Text, IntWritable, Text, FloatWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float sum = 0;
            float count = 0;

            for (IntWritable value : values) {
                sum += value.get();//计算总分
                count++;//统计总科目  
            }
            float avg = sum / count;
            context.write(key, new FloatWritable(avg));//输出学生姓名和平均值
        }

    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "avgScore");
        job.setJarByClass(MyAvgScore.class);

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

        args = MapReduceUtil.agrsIO(MyAvgScore.class, args);

        int ret = ToolRunner.run(new MyAvgScore(), args);
        System.exit(ret);
    }
} 