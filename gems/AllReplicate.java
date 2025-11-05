import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
// For Point2D
import java.awt.geom.Point2D;

/**
 * Implements the All-Replicate multi-way spatial join.
 * Based on Algorithm 2 in the paper.
 */
public class AllReplicate {

    // Configuration keys for grid parameters
    public static final String GRID_ROWS = "spatial.grid.rows";
    public static final String GRID_COLS = "spatial.grid.cols";
    public static final String TOTAL_WIDTH = "spatial.total.width";
    public static final String TOTAL_HEIGHT = "spatial.total.height";

    /**
     * Mapper: Replicates each rectangle to its first quadrant cells.
     * Implements Map phase of Algorithm 2.
     */
    public static class AllReplicateMapper extends Mapper<Object, Text, IntWritable, RectangleData> {

        private int gridRows, gridCols;
        private double totalWidth, totalHeight;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            gridRows = conf.getInt(GRID_ROWS, 1);
            gridCols = conf.getInt(GRID_COLS, 1);
            totalWidth = conf.getDouble(TOTAL_WIDTH, 1.0);
            totalHeight = conf.getDouble(TOTAL_HEIGHT, 1.0);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            RectangleData rectData = RectangleData.fromText(value.toString());
            if (rectData == null) return;

            // Replicate operation: Get all cells in the first quadrant (C_f(p)) [cite: 1301]
            List<Integer> cells = rectData.getRect().getFirstQuadrantCells(gridRows, gridCols, totalWidth, totalHeight);

            for (int cellId : cells) {
                context.write(new IntWritable(cellId), rectData);
            }
        }
    }

    /**
     * Reducer: Performs local 4-way join for all rectangles in its cell.
     * Implements Reduce phase of Algorithm 2.
     */
    public static class AllReplicateReducer extends Reducer<IntWritable, RectangleData, Text, NullWritable> {

        private int gridRows, gridCols;
        private double totalWidth, totalHeight;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            gridRows = conf.getInt(GRID_ROWS, 1);
            gridCols = conf.getInt(GRID_COLS, 1);
            totalWidth = conf.getDouble(TOTAL_WIDTH, 1.0);
            totalHeight = conf.getDouble(TOTAL_HEIGHT, 1.0);
        }

        @Override
        public void reduce(IntWritable cellId, Iterable<RectangleData> values, Context context) throws IOException, InterruptedException {
            
            // Partition rectangles by relation (P, Q, R, S) 
            List<RectangleData> pList = new ArrayList<>();
            List<RectangleData> qList = new ArrayList<>();
            List<RectangleData> rList = new ArrayList<>();
            List<RectangleData> sList = new ArrayList<>();

            for (RectangleData val : values) {
                // Must create a deep copy, as Hadoop reuses the value object
                RectangleData copy = new RectangleData(val.getRelation(), val.getRect());
                String rel = copy.getRelation().toString();
                if (rel.equals("P")) pList.add(copy);
                else if (rel.equals("Q")) qList.add(copy);
                else if (rel.equals("R")) rList.add(copy);
                else if (rel.equals("S")) sList.add(copy);
            }

            // Perform local 4-way nested-loop join [cite: 1302]
            for (RectangleData p : pList) {
                for (RectangleData q : qList) {
                    if (p.getRect().overlaps(q.getRect())) {
                        for (RectangleData r : rList) {
                            if (q.getRect().overlaps(r.getRect())) {
                                for (RectangleData s : sList) {
                                    if (r.getRect().overlaps(s.getRect())) {
                                        
                                        // Duplicate Avoidance [cite: 1197, 1310]
                                        // We output the tuple only if the midpoint of the
                                        // "middle" (q,r) overlap falls within this cell.
                                        Point2D.Double mid = q.getRect().getOverlapMidpoint(r.getRect());
                                        
                                        if (Rectangle.isPointInCell(mid, cellId.get(), gridRows, gridCols, totalWidth, totalHeight)) {
                                            String output = "<" + p + " | " + q + " | " + r + " | " + s + ">";
                                            context.write(new Text(output), NullWritable.get());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Driver program
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("Usage: AllReplicate <input> <output> <gridRows> <gridCols> <totalWidth> <totalHeight>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.setInt(GRID_ROWS, Integer.parseInt(args[2]));
        conf.setInt(GRID_COLS, Integer.parseInt(args[3]));
        conf.setDouble(TOTAL_WIDTH, Double.parseDouble(args[4]));
        conf.setDouble(TOTAL_HEIGHT, Double.parseDouble(args[5]));

        Job job = Job.getInstance(conf, "All-Replicate Join");
        job.setJarByClass(AllReplicate.class);
        job.setMapperClass(AllReplicateMapper.class);
        job.setReducerClass(AllReplicateReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(RectangleData.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}