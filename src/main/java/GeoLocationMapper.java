import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * Created by Rajiv on 4/6/17.
 */
public class GeoLocationMapper extends Mapper<Text, CustomLogRecord, Text, IntWritable> {


    public String status;

    public void map(Text key, CustomLogRecord value, Context context)
            throws IOException, InterruptedException {

        status = value.getResponseCode().trim();

        if (status.equals("200")) {
            context.getCounter("200 Status", "OK").increment(1l);
        } else if (status.equals("404")) {
            context.getCounter("404", "Fail").increment(1l);
        } else if (status.equals("503")) {
            context.getCounter("503", "UnKnown").increment(1l);
        } else {
            System.out.println("Status code unknown");
        }
        context.write(new Text(value.getResponseCode()), new IntWritable(1));
    }
}
