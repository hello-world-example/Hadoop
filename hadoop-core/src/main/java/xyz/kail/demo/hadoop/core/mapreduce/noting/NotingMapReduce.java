package xyz.kail.demo.hadoop.core.mapreduce.noting;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Created by kail on 2018/3/10.
 */
public class NotingMapReduce extends Configured implements Tool {

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
        job.setJarByClass(NotingMapReduce.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        String localFile = NotingMapReduce.class.getResource("").toString()
//                .replace("file:", "")
                .replace("target/classes/", "src/main/java/");

        if (args.length <= 0) {
            String input = localFile + "input";
            String output = localFile + "output";


            File outputFile = new File(output);
            FileUtils.deleteDirectory(outputFile);


            List<String> params = Arrays.asList(
                    input, output
            );
            args = params.toArray(new String[params.size()]);
        }

        int exitCode = ToolRunner.run(new NotingMapReduce(), args);

        System.exit(exitCode);
    }

}
