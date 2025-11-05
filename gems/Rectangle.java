import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
// For Point2D
import java.awt.geom.Point2D;

/**
 * Represents a spatial rectangle (MBR) that is WritableComparable.
 * Based on the MBR representation in the paper [cite: 750-753].
 */
public class Rectangle implements WritableComparable<Rectangle> {

    public double xl, yb, xr, yt; // left-x, bottom-y, right-x, top-y

    public Rectangle() {}

    public Rectangle(double xl, double yb, double xr, double yt) {
        this.xl = xl;
        this.yb = yb;
        this.xr = xr;
        this.yt = yt;
    }

    // --- WritableComparable Methods ---

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(xl);
        out.writeDouble(yb);
        out.writeDouble(xr);
        out.writeDouble(yt);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        xl = in.readDouble();
        yb = in.readDouble();
        xr = in.readDouble();
        yt = in.readDouble();
    }

    @Override
    public int compareTo(Rectangle o) {
        if (this.xl != o.xl) return Double.compare(this.xl, o.xl);
        if (this.yb != o.yb) return Double.compare(this.yb, o.yb);
        if (this.xr != o.xr) return Double.compare(this.xr, o.xr);
        return Double.compare(this.yt, o.yt);
    }

    // --- Spatial Logic Methods ---

    /**
     * Checks if this rectangle overlaps with another.
     * Implements Definition 2 (Overlap)[cite: 758].
     */
    public boolean overlaps(Rectangle o) {
        return !(this.xr < o.xl || this.xl > o.xr || this.yt < o.yb || this.yb > o.yt);
    }

    /**
     * Gets the cell ID containing the lower-left corner (lb(r)).
     * [cite: 1289]
     */
    public int getLowerLeftCell(int gridRows, int gridCols, double totalWidth, double totalHeight) {
        double cellWidth = totalWidth / gridCols;
        double cellHeight = totalHeight / gridRows;
        int col = (int) (this.xl / cellWidth);
        int row = (int) (this.yb / cellHeight);
        // Clamp to grid bounds
        if (col >= gridCols) col = gridCols - 1;
        if (row >= gridRows) row = gridRows - 1;
        return row * gridCols + col;
    }

    /**
     * Implements the "split" operation from C-Rep Mapper 1[cite: 1463, 1492].
     * Finds all cells this rectangle overlaps.
     */
    public List<Integer> getOverlappingCells(int gridRows, int gridCols, double totalWidth, double totalHeight) {
        List<Integer> cells = new ArrayList<>();
        double cellWidth = totalWidth / gridCols;
        double cellHeight = totalHeight / gridRows;

        int startCol = (int) (this.xl / cellWidth);
        int endCol = (int) (this.xr / cellWidth);
        int startRow = (int) (this.yb / cellHeight);
        int endRow = (int) (this.yt / cellHeight);

        // Clamp to grid bounds
        if (endCol >= gridCols) endCol = gridCols - 1;
        if (endRow >= gridRows) endRow = gridRows - 1;
        
        for (int r = startRow; r <= endRow; r++) {
            for (int c = startCol; c <= endCol; c++) {
                cells.add(r * gridCols + c);
            }
        }
        return cells;
    }

    /**
     * Implements the "First Quadrant" (C_f(r)) replication.
     * Used by All-Replicate [cite: 1301] and C-Rep Mapper 2[cite: 1646].
     */
    public List<Integer> getFirstQuadrantCells(int gridRows, int gridCols, double totalWidth, double totalHeight) {
        List<Integer> cells = new ArrayList<>();
        int lbCellId = getLowerLeftCell(gridRows, gridCols, totalWidth, totalHeight);
        int startCol = lbCellId % gridCols;
        int startRow = lbCellId / gridCols;

        for (int r = startRow; r < gridRows; r++) {
            for (int c = startCol; c < gridCols; c++) {
                cells.add(r * gridCols + c);
            }
        }
        return cells;
    }

    /**
     * Checks if the rectangle crosses the boundary of a *specific* cell.
     * Used by C-Rep Reducer 1[cite: 1467].
     */
    public boolean crossesCellBoundary(int cellId, int gridRows, int gridCols, double totalWidth, double totalHeight) {
        double cellWidth = totalWidth / gridCols;
        double cellHeight = totalHeight / gridRows;
        int cellCol = cellId % gridCols;
        int cellRow = cellId / gridCols;

        double cellMinX = cellCol * cellWidth;
        double cellMaxX = (cellCol + 1) * cellWidth;
        double cellMinY = cellRow * cellHeight;
        double cellMaxY = (cellRow + 1) * cellHeight;

        // Check if rectangle extends beyond the cell's boundaries
        return (this.xl < cellMinX || this.xr > cellMaxX || this.yb < cellMinY || this.yt > cellMaxY);
    }

    /**
     * Calculates the midpoint of the overlap region with another rectangle.
     * Used for duplicate avoidance[cite: 1197].
     */
    public Point2D.Double getOverlapMidpoint(Rectangle o) {
        double midX = (Math.max(this.xl, o.xl) + Math.min(this.xr, o.xr)) / 2.0;
        double midY = (Math.max(this.yb, o.yb) + Math.min(this.yt, o.yt)) / 2.0;
        return new Point2D.Double(midX, midY);
    }

    /**
     * Checks if a point is contained within a specific cell's boundaries.
     * Used for duplicate avoidance[cite: 1199].
     */
    public static boolean isPointInCell(Point2D.Double p, int cellId, int gridRows, int gridCols, double totalWidth, double totalHeight) {
        double cellWidth = totalWidth / gridCols;
        double cellHeight = totalHeight / gridRows;
        int cellCol = cellId % gridCols;
        int cellRow = cellId / gridCols;

        double cellMinX = cellCol * cellWidth;
        double cellMaxX = (cellCol + 1) * cellWidth;
        double cellMinY = cellRow * cellHeight;
        double cellMaxY = (cellRow + 1) * cellHeight;

        return (p.x >= cellMinX && p.x < cellMaxX && p.y >= cellMinY && p.y < cellMaxY);
    }
    
    // --- Overrides ---

    @Override
    public String toString() {
        return xl + "," + yb + "," + xr + "," + yt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Rectangle rectangle = (Rectangle) o;
        return Double.compare(rectangle.xl, xl) == 0 &&
                Double.compare(rectangle.yb, yb) == 0 &&
                Double.compare(rectangle.xr, xr) == 0 &&
                Double.compare(rectangle.yt, yt) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(xl, yb, xr, yt);
    }
}