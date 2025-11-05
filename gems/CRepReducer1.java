import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reducer for Job 1 of Controlled-Replicate.
 * Performs analysis, generates early output, and marks rectangles for replication.
 * Implements Algorithm 3, Reducer 1.
 */
public class CRepReducer1 extends Reducer<IntWritable, RectangleData, Text, NullWritable> {

    private int gridRows, gridCols;
    private double totalWidth, totalHeight;
    private MultipleOutputs<Text, NullWritable> mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        gridRows = conf.getInt(AllReplicate.GRID_ROWS, 1);
        gridCols = conf.getInt(AllReplicate.GRID_COLS, 1);
        totalWidth = conf.getDouble(AllReplicate.TOTAL_WIDTH, 1.0);
        totalHeight = conf.getDouble(AllReplicate.TOTAL_HEIGHT, 1.0);
        mos = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(IntWritable cellId, Iterable<RectangleData> values, Context context) throws IOException, InterruptedException {
        
        List<RectangleData> pList = new ArrayList<>();
        List<RectangleData> qList = new ArrayList<>();
        List<RectangleData> rList = new ArrayList<>();
        List<RectangleData> sList = new ArrayList<>();

        for (RectangleData val : values) {
            RectangleData copy = new RectangleData(val.getRelation(), val.getRect());
            String rel = copy.getRelation().toString();
            if (rel.equals("P")) pList.add(copy);
            else if (rel.equals("Q")) qList.add(copy);
            else if (rel.equals("R")) rList.add(copy);
            else if (rel.equals("S")) sList.add(copy);
        }

        Set<RectangleData> markedRects = new HashSet<>();

        // Analysis of (q, r) pairs
        for (RectangleData q : qList) {
            for (RectangleData r : rList) {
                if (q.getRect().overlaps(r.getRect())) {
                    
                    boolean qCrosses = q.getRect().crossesCellBoundary(cellId.get(), gridRows, gridCols, totalWidth, totalHeight);
                    boolean rCrosses = r.getRect().crossesCellBoundary(cellId.get(), gridRows, gridCols, totalWidth, totalHeight);

                    // Case 1: Early Output. Neither q nor r crosses the boundary.
                    if (!qCrosses && !rCrosses) { //
                        for (RectangleData p : pList) {
                            if (p.getRect().overlaps(q.getRect())) {
                                for (RectangleData s : sList) {
                                    if (r.getRect().overlaps(s.getRect())) {
                                        String output = "<" + p + " | " + q + " | " + r + " | " + s + ">";
                                        // Emit to "earlyoutput" file
                                        mos.write("earlyoutput", new Text(output), NullWritable.get());
                                    }
                                }
                            }
                        }
                    } 
                    // Case 2: Mark for Replication. One or both cross.
                    else { //
                        markedRects.add(q);
                        markedRects.add(r);
                        // Mark associated P and S rectangles
                        for (RectangleData p : pList) {
                            if (p.getRect().overlaps(q.getRect())) markedRects.add(p);
                        }
                        for (RectangleData s : sList) {
                            if (r.getRect().overlaps(s.getRect())) markedRects.add(s);
                        }
                    }
                }
            }
        }
        
        // Mark any other rectangles that cross the boundary
        for (RectangleData p : pList) if (p.getRect().crossesCellBoundary(cellId.get(), gridRows, gridCols, totalWidth, totalHeight)) markedRects.add(p);
        for (RectangleData q : qList) if (q.getRect().crossesCellBoundary(cellId.get(), gridRows, gridCols, totalWidth, totalHeight)) markedRects.add(q);
        for (RectangleData r : rList) if (r.getRect().crossesCellBoundary(cellId.get(), gridRows, gridCols, totalWidth, totalHeight)) markedRects.add(r);
        for (RectangleData s : sList) if (s.getRect().crossesCellBoundary(cellId.get(), gridRows, gridCols, totalWidth, totalHeight)) markedRects.add(s);

        // Emit marked rectangles using Project(r) logic [cite: 1625, 1492]
        // Project(r) = only emit if this cell (reducer) contains the lb(r).
        for (RectangleData rect : markedRects) {
            int lbCell = rect.getRect().getLowerLeftCell(gridRows, gridCols, totalWidth, totalHeight);
            if (lbCell == cellId.get()) {
                // Emit to "marked" file for Job 2
                mos.write("marked", new Text(rect.toString()), NullWritable.get());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}