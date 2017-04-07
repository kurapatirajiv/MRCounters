import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by Rajiv on 4/6/17.
 */
public class GeoLocationDriver {

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException, URISyntaxException {

        if (args.length != 2) {
            System.out.println("Not enough arguments");
            System.exit(-1);
        }

        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create configuration
        Configuration conf = new Configuration();
        Job job = new Job(conf, "IPGeo Version 2");


        job.setJarByClass(GeoLocationDriver.class);

        // Setup MapReduce
        job.setMapperClass(GeoLocationMapper.class);
        job.setNumReduceTasks(0);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input
        FileInputFormat.addInputPath(job, inputPath);
        job.setInputFormatClass(CustomLongInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, outputDir);
        job.setOutputFormatClass(TextOutputFormat.class);

        //Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);

        job.waitForCompletion(false);

        long goodResp = job.getCounters().findCounter("200 Status", "OK").getValue();
        long pgNotFound = job.getCounters().findCounter("404", "Fail").getValue();
        long unKnownResp = job.getCounters().findCounter("503", "UnKnown").getValue();
        System.out.println("Num of 200 Statuses = " + goodResp);
        System.out.println("Num of 404 Statuses = " + pgNotFound);
        System.out.println("Num of 503 Statuses = " + unKnownResp);

    }

}
