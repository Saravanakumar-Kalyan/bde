import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.List;

public class VanillaMultiwayJoin {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Vanilla Join")
                .master("local[*]") // Use local mode for testing
                .getOrCreate();

        // 1. Define Schemas
        StructType schemaR = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("A", DataTypes.IntegerType, true),
                DataTypes.createStructField("B", DataTypes.IntegerType, true)
        });

        StructType schemaS = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("B_S", DataTypes.IntegerType, true),
                DataTypes.createStructField("E", DataTypes.IntegerType, true),
                DataTypes.createStructField("C", DataTypes.IntegerType, true)
        });

        StructType schemaT = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("C_T", DataTypes.IntegerType, true),
                DataTypes.createStructField("D", DataTypes.IntegerType, true)
        });

        // 2. Create Skewed Sample Data
        // We will create a "Heavy Hitter" (HH) on B=1 and C=10
        List<Row> dataR = new ArrayList<>();
        List<Row> dataS = new ArrayList<>();
        List<Row> dataT = new ArrayList<>();

        // Create 1,000,000 rows where B=1 (the HH)
        for (int i = 0; i < 1000000; i++) {
            dataR.add(RowFactory.create(i, 1)); // (A, B)
        }
        // Create 100,000 non-HH rows
        for (int i = 0; i < 100000; i++) {
            dataR.add(RowFactory.create(i, i + 2)); // (A, B)
            dataS.add(RowFactory.create(i + 2, i * 2, i + 5)); // (B_S, E, C)
            dataT.add(RowFactory.create(i + 5, i * 3)); // (C_T, D)
        }

        // Create 1,000,000 rows where B_S=1 and C=10 (double HH)
        for (int i = 0; i < 1000000; i++) {
            dataS.add(RowFactory.create(1, i, 10)); // (B_S, E, C)
        }
        
        // Create 1,000,000 rows where C_T=10 (the HH)
        for (int i = 0; i < 1000000; i++) {
            dataT.add(RowFactory.create(10, i)); // (C_T, D)
        }

        Dataset<Row> r = spark.createDataFrame(dataR, schemaR);
        Dataset<Row> s = spark.createDataFrame(dataS, schemaS);
        Dataset<Row> t = spark.createDataFrame(dataT, schemaT);

        // 3. Perform the Vanilla Multi-Way Join (Join Cascade)
        System.out.println("Starting vanilla join...");
        long startTime = System.nanoTime();

        Dataset<Row> joined = r
            .join(s, r.col("B").equalTo(s.col("B_S")))
            .join(t, s.col("C").equalTo(t.col("C_T")));

        // Force execution
        joined.count(); 

        long endTime = System.nanoTime();
        System.out.println("Vanilla join completed in " + (endTime - startTime) / 1_000_000 + " ms");

        // To see the query plan:
        // joined.explain(true);

        /*
         * When you run this, go to the Spark UI (http://localhost:4040).
         * Look at the "Stages" tab. You will see one or two join stages.
         * Click on the stage and look at the "Aggregated Metrics".
         * You will see that tasks for one partition (the one with key B=1)
         * take *far* longer than all the others. This is data skew.
         */

        spark.stop();
    }
}