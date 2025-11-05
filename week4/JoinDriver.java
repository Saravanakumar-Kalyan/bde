import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class JoinDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: JoinDriver <customers_path> <orders_path> <partition_file_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Range Partitioner Join");

        job.setJarByClass(JoinDriver.class);
        
        // Set Mapper and Reducer
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        // Set the custom Partitioner
        job.setPartitionerClass(RangePartitioner.class);

        job.addCacheFile(new URI(args[2]));

        job.setNumReduceTasks(3);

        // Set Mapper and Reducer output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input paths for both datasets
        FileInputFormat.addInputPath(job, new Path(args[0])); // customers
        FileInputFormat.addInputPath(job, new Path(args[1])); // orders
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}