import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.List;

/**
 * Mapper for Job 1 of Controlled-Replicate.
 * Implements the "split" operation[cite: 1463, 1492].
 */
public class CRepMapper1 extends Mapper<Object, Text, IntWritable, RectangleData> {

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
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        RectangleData rectData = RectangleData.fromText(value.toString());
        if (rectData == null) return;

        // "split" operation: Get all *overlapping* cells
        List<Integer> cells = rectData.getRect().getOverlappingCells(gridRows, gridCols, totalWidth, totalHeight);

        for (int cellId : cells) {
            context.write(new IntWritable(cellId), rectData);
        }
    }
}