import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.List;

/**
 * Mapper for Job 2 of Controlled-Replicate.
 * Reads marked rectangles and performs C_f(r) replication.
 * Implements Algorithm 3, Map 2.
 */
public class CRepMapper2 extends Mapper<Text, NullWritable, IntWritable, RectangleData> {

    private int gridRows, gridCols;
    private double totalWidth, totalHeight;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        gridRows = conf.getInt(AllReplicate.GRID_ROWS, 1);
        gridCols = conf.getInt(AllReplicate.GRID_COLS, 1);
        totalWidth = conf.getDouble(AllReplicate.TOTAL_WIDTH, 1.0);
        totalHeight = conf.getDouble(AllReplicate.TOTAL_HEIGHT, 1.0);
    }

    @Override
    public void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException {
        // Input key is the text representation from CRepReducer1
        RectangleData rectData = RectangleData.fromText(key.toString());
        if (rectData == null) return;

        // Replicate operation (C_cr is equivalent to C_f) [cite: 1646, 1492]
        List<Integer> cells = rectData.getRect().getFirstQuadrantCells(gridRows, gridCols, totalWidth, totalHeight);

        for (int cellId : cells) {
            context.write(new IntWritable(cellId), rectData);
        }
    }
}