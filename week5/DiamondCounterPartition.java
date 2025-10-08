import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DiamondCounterPartition {

    public static class PartitionMap extends Mapper<Object, Text, Text, Text> {
        private int p;

        @Override
        protected void setup(Context context) {
            p = context.getConfiguration().getInt("partitions.p", 10);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split("\\s+");
            if (nodes.length < 2) return;

            long u = Long.parseLong(nodes[0]);
            long v = Long.parseLong(nodes[1]);

            long partU = u % p;
            long partV = v % p;

            Set<Long> partitions = new TreeSet<>();
            partitions.add(partU);
            partitions.add(partV);

            for (long k = 0; k < p; k++) {
                if (partitions.contains(k)) continue;
                for (long l = k + 1; l < p; l++) {
                    if (partitions.contains(l)) continue;
                    
                    TreeSet<Long> keyParts = new TreeSet<>(partitions);
                    keyParts.add(k);
                    keyParts.add(l);
                    
                    StringJoiner sj = new StringJoiner("_");
                    keyParts.forEach(part -> sj.add(String.valueOf(part)));
                    
                    context.write(new Text(sj.toString()), value);
                }
            }
        }
    }

    public static class PartitionReduce extends Reducer<Text, Text, Text, DoubleWritable> {
        private int p;
        private final Text outputKey = new Text("diamond_count");

        @Override
        protected void setup(Context context) {
            p = context.getConfiguration().getInt("partitions.p", 10);
        }
        
        private long nCr(int n, int r) {
            if (r < 0 || r > n) {
                return 0;
            }
            if (r == 0 || r == n) {
                return 1;
            }
            if (r > n / 2) {
                r = n - r;
            }
            long res = 1;
            for (int i = 1; i <= r; i++) {
                res = res * (n - i + 1) / i;
            }
            return res;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Long, Set<Long>> adjList = new HashMap<>();
            Set<String> edges = new HashSet<>();
            double localWeightedCount = 0.0;

            for (Text edgeText : values) {
                String edgeStr = edgeText.toString();
                edges.add(edgeStr);
                String[] nodes = edgeStr.split("\\s+");
                long u = Long.parseLong(nodes[0]);
                long v = Long.parseLong(nodes[1]);
                adjList.computeIfAbsent(u, k -> new HashSet<>()).add(v);
                adjList.computeIfAbsent(v, k -> new HashSet<>()).add(u);
            }

            for (String edgeStr : edges) {
                String[] nodes = edgeStr.split("\\s+");
                long u = Long.parseLong(nodes[0]);
                long v = Long.parseLong(nodes[1]);

                Set<Long> neighborsU = adjList.getOrDefault(u, Collections.emptySet());
                Set<Long> neighborsV = adjList.getOrDefault(v, Collections.emptySet());
                
                Set<Long> commonNeighbors = new HashSet<>(neighborsU);
                commonNeighbors.retainAll(neighborsV);

                if (commonNeighbors.size() > 1) {
                    List<Long> commonList = new ArrayList<>(commonNeighbors);
                    for (int i = 0; i < commonList.size(); i++) {
                        for (int j = i + 1; j < commonList.size(); j++) {
                            long w = commonList.get(i);
                            long x = commonList.get(j);

                            Set<Long> distinctPartitions = new HashSet<>();
                            distinctPartitions.add(u % p);
                            distinctPartitions.add(v % p);
                            distinctPartitions.add(w % p);
                            distinctPartitions.add(x % p);
                            
                            int k = distinctPartitions.size();
                            long z = nCr(p - k, 4 - k);

                            if (z > 0) {
                                localWeightedCount += 1.0 / z;
                            }
                        }
                    }
                }
            }

            if (localWeightedCount > 0) {
                context.write(outputKey, new DoubleWritable(localWeightedCount));
            }
        }
    }

    public static class SumReduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double total = 0.0;
            for (DoubleWritable val : values) {
                total += val.get();
            }
            context.write(new Text("Total Diamonds Found (Weighted Sum):"), new DoubleWritable(total));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: DiamondCounterPartition <input path> <output path> <p>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("partitions.p", Integer.parseInt(args[2]));

        Path intermediatePath = new Path(args[1] + "_intermediate");
        Path finalOutputPath = new Path(args[1]);

        System.out.println("Starting Job 1: Partition and Count");
        Job job1 = Job.getInstance(conf, "Job 1: Partition and Count");
        job1.setJarByClass(DiamondCounterPartition.class);
        job1.setMapperClass(PartitionMap.class);
        job1.setReducerClass(PartitionReduce.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, intermediatePath);
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        System.out.println("Starting Job 2: Final Summation");
        Job job2 = Job.getInstance(conf, "Job 2: Final Summation");
        job2.setJarByClass(DiamondCounterPartition.class);
        job2.setReducerClass(SumReduce.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);
        
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}