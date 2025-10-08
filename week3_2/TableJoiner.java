import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableJoiner {

    public static class RegistrationMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 5) {
                String id = fields[0].replaceAll("\"", "");
                String st_id = fields[3].replaceAll("\"", "");
                String sub_id = fields[4].replaceAll("\"", "");

                joinKey.set(id);

                outputValue.set("R," + st_id + "," + sub_id);
                context.write(joinKey, outputValue);
            }
        }
    }

    public static class GradesMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length == 6) {
                joinKey.set(fields[4]);

                outputValue.set("G," + fields[5]);
                context.write(joinKey, outputValue);
            }
        }
    }

    public static class SubjectMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            // System.out.println(fields.length);
            // if (fields.length == 11) {
            //     System.out.println("SubjectMapper: " + value.toString());
            // }
            // if (fields.length == 3) {
            // }
            //   3 4 8 11
            String sem = fields[3].replaceAll("\"", "");
            String year = fields[4].replaceAll("\"", "");
            String sub_id = fields[8].replaceAll("\"", "");
            String credits = fields[11].replaceAll("\"", "");

            joinKey.set(sub_id);
            outputValue.set("S," + sem + "," + year + "," + credits);
            context.write(joinKey, outputValue);
        }
    }

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text joinKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            // id stid subid grade
            // if (fields.length >= 2) {
            joinKey.set(fields[2]);

            outputValue.set("J," + fields[1] + "," + fields[3]);
            context.write(joinKey, outputValue);
            // }
        }
    }


    public static class RegGradeReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> regRecords = new ArrayList<>();
            List<String> gradesRecords = new ArrayList<>();

            for (Text value : values) {
                String record = value.toString();
                if (record.startsWith("R,")) {
                    regRecords.add(record.substring(2));
                } else if (record.startsWith("G,")) {
                    gradesRecords.add(record.substring(2));
                } 
                // else if (record.startsWith("S,")) {
                //     subjectRecords.add(record.substring(2));
                // }
            }

            if (!regRecords.isEmpty() && !gradesRecords.isEmpty()) {
                for (String regData : regRecords) {
                    for (String gradesData : gradesRecords) {
                        // for (String subjectData: subjectRecords) {
                        String outputLine = key.toString() + "," + regData + "," + gradesData;
                        context.write(new Text(outputLine), null);
                        // }
                    }
                }
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> joinRecords = new ArrayList<>();
            List<String> subjectRecords = new ArrayList<>();

            for (Text value : values) {
                String record = value.toString();
                if (record.startsWith("J,")) {
                    joinRecords.add(record.substring(2)); // remove the tag
                } else if (record.startsWith("S,")) {
                    subjectRecords.add(record.substring(2)); // remove the tag
                    // System.out.println(record);
                }
            }

            if (!joinRecords.isEmpty() && !subjectRecords.isEmpty()) {
                for (String joinData : joinRecords) {
                    for (String subjectData : subjectRecords) {
                        String outputLine = key.toString() + "," + joinData + "," + subjectData;
                        context.write(new Text(outputLine), null);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5) {
            System.err.println("Usage: TableJoiner <registration input path> <grades input path> <subject input path> <intermediate path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Join T1 and T2");
        job1.setJarByClass(TableJoiner.class);
        
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RegistrationMapper.class); // T1 input
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, GradesMapper.class); // T2 input

        job1.setReducerClass(RegGradeReducer.class);

        // job1.setMapperClass(Mapper1.class);
        // job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[3]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Join T3");
        job2.setJarByClass(TableJoiner.class);

        MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class, SubjectMapper.class); // T3 input
        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, JoinMapper.class); // Intermediate output

        job2.setReducerClass(JoinReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

// subid, student id, grade, year, sem, credits