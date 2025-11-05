import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A Writable wrapper class to hold a Rectangle and its relation (P, Q, R, S).
 */
public class RectangleData implements Writable {

    private Text relation = new Text();
    private Rectangle rect = new Rectangle();

    public RectangleData() {}

    public RectangleData(RectangleData other) {
        this.relation.set(other.relation);
        this.rect = new Rectangle(other.rect.xl, other.rect.yb, other.rect.xr, other.rect.yt);
    }

    public RectangleData(Text relation, Rectangle rect) {
        this.relation.set(relation);
        this.rect = rect;
    }

    public Text getRelation() { return relation; }
    public Rectangle getRect() { return rect; }

    @Override
    public void write(DataOutput out) throws IOException {
        relation.write(out);
        rect.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        relation.readFields(in);
        rect.readFields(in);
    }

    /**
     * Parses a line from the input file.
     * Format: Relation,xl,yb,xr,yt (e.g., "P,10,10,20,20")
     */
    public static RectangleData fromText(String line) {
        try {
            String[] parts = line.split(",");
            Text rel = new Text(parts[0]);
            Rectangle r = new Rectangle(
                Double.parseDouble(parts[1]),
                Double.parseDouble(parts[2]),
                Double.parseDouble(parts[3]),
                Double.parseDouble(parts[4])
            );
            return new RectangleData(rel, r);
        } catch (Exception e) {
            System.err.println("Error parsing line: " + line);
            return null; // Handle error gracefully
        }
    }

    @Override
    public String toString() {
        return relation.toString() + "," + rect.toString();
    }
}