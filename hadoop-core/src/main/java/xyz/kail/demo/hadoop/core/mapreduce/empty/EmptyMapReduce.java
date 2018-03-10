package xyz.kail.demo.hadoop.core.mapreduce.empty;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import xyz.kail.demo.hadoop.core.util.MapReduceUtil;

/**
 * Created by kail on 2018/3/10.
 */
public class EmptyMapReduce extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (2 != args.length) {
            System.err.println("");
            System.err.println("缺少必要参数：" + getClass());
            System.err.println("");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = Job.getInstance(getConf());

        job.setJarByClass(EmptyMapReduce.class); // 可选

        /*
         * 必选
         */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        args = MapReduceUtil.agrsIO(EmptyMapReduce.class, args);

        int exitCode = ToolRunner.run(new EmptyMapReduce(), args);

        System.exit(exitCode);
    }

}
