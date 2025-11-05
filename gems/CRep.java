import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Driver for the two-stage Controlled-Replicate (C-Rep) join.
 * Based on Algorithm 3.
 */
public class CRep {

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: CRep <input> <output> <gridRows> <gridCols> <totalWidth> <totalHeight>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        String gridRows = args[2];
        String gridCols = args[3];
        String totalWidth = args[4];
        String totalHeight = args[5];

        Path tempPath = new Path(outputPath + "_temp");
        Path finalPath = new Path(outputPath + "_final");

        Configuration conf = new Configuration();
        conf.setInt(AllReplicate.GRID_ROWS, Integer.parseInt(gridRows));
        conf.setInt(AllReplicate.GRID_COLS, Integer.parseInt(gridCols));
        conf.setDouble(AllReplicate.TOTAL_WIDTH, Double.parseDouble(totalWidth));
        conf.setDouble(AllReplicate.TOTAL_HEIGHT, Double.parseDouble(totalHeight));

        // --- Job 1: Analysis (CRepMapper1, CRepReducer1) ---
        Job job1 = Job.getInstance(conf, "C-Rep Job 1: Analyze");
        job1.setJarByClass(CRep.class);
        job1.setMapperClass(CRepMapper1.class);
        job1.setReducerClass(CRepReducer1.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(RectangleData.class);
        
        // Reducer output is text
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, tempPath);

        // Define the two named outputs for Reducer 1
        MultipleOutputs.addNamedOutput(job1, "earlyoutput", TextOutputFormat.class, Text.class, NullWritable.class);
        MultipleOutputs.addNamedOutput(job1, "marked", TextOutputFormat.class, Text.class, NullWritable.class);

        if (!job1.waitForCompletion(true)) {
            System.err.println("C-Rep Job 1 failed!");
            System.exit(1);
        }

        // --- Job 2: Replicate & Join (CRepMapper2, AllReplicateReducer) ---
        Job job2 = Job.getInstance(conf, "C-Rep Job 2: Replicate-Join");
        job2.setJarByClass(CRep.class);
        job2.setMapperClass(CRepMapper2.class);
        
        // Use the *same reducer* as All-Replicate [cite: 1628, 1310]
        job2.setReducerClass(AllReplicate.AllReplicateReducer.class); 

        // Mapper 2's input format (from "marked" files)
        job2.setInputFormatClass(TextInputFormat.class);
        
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(RectangleData.class);
        
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        // Input for Job 2 is the "marked" output from Job 1
        FileInputFormat.addInputPath(job2, new Path(tempPath + "/marked-r-*"));
        FileOutputFormat.setOutputPath(job2, finalPath);

        boolean success = job2.waitForCompletion(true);
        
        if (success) {
            System.out.println("C-Rep job successful.");
            System.out.println("Early output (if any) is in: " + tempPath + "/earlyoutput-r-*");
            System.out.println("Replicated output is in: " + finalPath);
            System.out.println("To get full results, run:");
            System.out.println("hadoop fs -cat " + tempPath + "/earlyoutput-r-* " + finalPath + "/part-r-*");
        } else {
            System.err.println("C-Rep Job 2 failed!");
        }

        System.exit(success ? 0 : 1);
    }
}