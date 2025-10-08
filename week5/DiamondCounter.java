import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class DiamondCounter {

    public static class PairWritable implements WritableComparable<PairWritable> {
        private LongWritable first;
        private LongWritable second;

        public PairWritable() {
            set(new LongWritable(), new LongWritable());
        }

        public PairWritable(long first, long second) {
            if (first < second) {
                set(new LongWritable(first), new LongWritable(second));
            } else {
                set(new LongWritable(second), new LongWritable(first));
            }
        }

        public void set(LongWritable first, LongWritable second) {
            this.first = first;
            this.second = second;
        }

        public LongWritable getFirst() {
            return first;
        }

        public LongWritable getSecond() {
            return second;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            first.write(out);
            second.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            first.readFields(in);
            second.readFields(in);
        }

        @Override
        public int compareTo(PairWritable other) {
            int cmp = first.compareTo(other.first);
            if (cmp != 0) {
                return cmp;
            }
            return second.compareTo(other.second);
        }

        @Override
        public int hashCode() {
            return first.hashCode() * 163 + second.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof PairWritable) {
                PairWritable other = (PairWritable) o;
                return first.equals(other.first) && second.equals(other.second);
            }
            return false;
        }

        @Override
        public String toString() {
            return first + "\t" + second;
        }
    }

    public static class WedgeMap extends Mapper<Object, Text, LongWritable, LongWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split("\\s+");
            if (nodes.length == 2) {
                long u = Long.parseLong(nodes[0]);
                long v = Long.parseLong(nodes[1]);
                if (u < v) {
                    context.write(new LongWritable(u), new LongWritable(v));
                } else {
                    context.write(new LongWritable(v), new LongWritable(u));
                }
            }
        }
    }

    public static class WedgeReduce extends Reducer<LongWritable, LongWritable, PairWritable, LongWritable> {
        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            List<Long> neighbors = new ArrayList<>();
            for (LongWritable val : values) {
                neighbors.add(val.get());
            }

            if (neighbors.size() > 1) {
                for (int i = 0; i < neighbors.size(); i++) {
                    for (int j = i + 1; j < neighbors.size(); j++) {
                        context.write(new PairWritable(neighbors.get(i), neighbors.get(j)), key);
                    }
                }
            }
        }
    }

    public static class TriangleMapEdges extends Mapper<Object, Text, PairWritable, Text> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split("\\s+");
            if (nodes.length == 2) {
                long u = Long.parseLong(nodes[0]);
                long v = Long.parseLong(nodes[1]);
                context.write(new PairWritable(u, v), new Text("$"));
            }
        }
    }

    public static class TriangleMapWedges extends Mapper<PairWritable, LongWritable, PairWritable, Text> {
        @Override
        public void map(PairWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, new Text("V" + value.get()));
        }
    }

    public static class TriangleReduce extends Reducer<PairWritable, Text, PairWritable, LongWritable> {
        @Override
        public void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean isEdge = false;
            List<Long> vertices = new ArrayList<>();
            for (Text val : values) {
                if (val.toString().equals("$")) {
                    isEdge = true;
                } else {
                    vertices.add(Long.parseLong(val.toString().substring(1)));
                }
            }

            if (isEdge && !vertices.isEmpty()) {
                long u = key.getFirst().get();
                long w = key.getSecond().get();
                for (Long v : vertices) {
                    long[] triangleNodes = {u, v, w};
                    Arrays.sort(triangleNodes);

                    context.write(new PairWritable(triangleNodes[0], triangleNodes[1]), new LongWritable(triangleNodes[2]));
                    context.write(new PairWritable(triangleNodes[0], triangleNodes[2]), new LongWritable(triangleNodes[1]));
                    context.write(new PairWritable(triangleNodes[1], triangleNodes[2]), new LongWritable(triangleNodes[0]));
                }
            }
        }
    }

    public static class DiamondReduce extends Reducer<PairWritable, LongWritable, Text, LongWritable> {
        private final Text outputKey = new Text("diamond_count");

        @Override
        public void reduce(PairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values) {
                count++;
            }

            if (count > 1) {
                long numDiamonds = count * (count - 1) / 2;
                context.write(outputKey, new LongWritable(numDiamonds));
            }
        }
    }

    public static class SumMap extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final Text outputKey = new Text("diamond_count");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2 && parts[0].equals("diamond_count")) {
                long count = Long.parseLong(parts[1]);
                context.write(outputKey, new LongWritable(count));
            }
        }
    }

    public static class SumReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            for (LongWritable val : values) {
                total += val.get();
            }
            context.write(new Text("Total Diamonds Found:"), new LongWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: DiamondCounter <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        String inputPath = args[0];
        String outputPath = args[1];

        Path wedgeOutputPath = new Path(outputPath + "_wedges");
        Path triangleOutputPath = new Path(outputPath + "_triangles");
        Path diamondCountOutputPath = new Path(outputPath + "_counts");
        Path finalOutputPath = new Path(outputPath + "_final");
        
        System.out.println("Starting Job 1: Wedge Generation");
        Job job1 = Job.getInstance(conf, "Job 1: Wedge Generation");
        job1.setJarByClass(DiamondCounter.class);
        job1.setMapperClass(WedgeMap.class);
        job1.setReducerClass(WedgeReduce.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(LongWritable.class);
        job1.setOutputKeyClass(PairWritable.class);
        job1.setOutputValueClass(LongWritable.class);
        job1.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);
        TextInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, wedgeOutputPath);
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        System.out.println("Starting Job 2: Triangle Identification");
        Job job2 = Job.getInstance(conf, "Job 2: Triangle Identification");
        job2.setJarByClass(DiamondCounter.class);
        job2.setReducerClass(TriangleReduce.class);
        job2.setMapOutputKeyClass(PairWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(PairWritable.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setOutputFormatClass(org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class);

        MultipleInputs.addInputPath(job2, new Path(inputPath), TextInputFormat.class, TriangleMapEdges.class);
        MultipleInputs.addInputPath(job2, wedgeOutputPath, org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class, TriangleMapWedges.class);
        FileOutputFormat.setOutputPath(job2, triangleOutputPath);
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
        
        System.out.println("Starting Job 3: Diamond Counting");
        Job job3 = Job.getInstance(conf, "Job 3: Diamond Counting");
        job3.setJarByClass(DiamondCounter.class);

        job3.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);

        job3.setReducerClass(DiamondReduce.class);
        job3.setMapOutputKeyClass(PairWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(LongWritable.class);

        org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.addInputPath(job3, triangleOutputPath);

        FileOutputFormat.setOutputPath(job3, diamondCountOutputPath);
        if (!job3.waitForCompletion(true)) {
            System.exit(1);
        }
        
        System.out.println("Starting Job 4: Final Summation");
        Job job4 = Job.getInstance(conf, "Job 4: Final Summation");
        job4.setJarByClass(DiamondCounter.class);

        job4.setMapperClass(SumMap.class);
        job4.setReducerClass(SumReduce.class);

        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(LongWritable.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(LongWritable.class);
        TextInputFormat.addInputPath(job4, diamondCountOutputPath);
        FileOutputFormat.setOutputPath(job4, finalOutputPath);
        
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}