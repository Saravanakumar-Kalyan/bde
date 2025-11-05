import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

public class SharesSkewJoin {

    // Define our Heavy Hitter (HH) values, as per 
    private static final int HH_B = 1;
    private static final int HH_C = 10;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SharesSkew Detailed Implementation")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "100") // Set a base partition number
                .getOrCreate();

        // --- 1. Generate Skewed Data ---
        System.out.println("Generating skewed data...");
        Map<String, Dataset<Row>> datasets = generateSkewedData(spark);
        Dataset<Row> R = datasets.get("R").cache();
        Dataset<Row> S = datasets.get("S").cache();
        Dataset<Row> T = datasets.get("T").cache();

        System.out.println("R count: " + R.count());
        System.out.println("S count: " + S.count());
        System.out.println("T count: " + T.count());

        // --- 2. Decompose into Residual Joins ---
        // As described in Section 5.1 and 7 [cite: 261, 372]
        // We will create 4 residual joins based on our two HH values (HH_B, HH_C)
        System.out.println("Decomposing into residual joins...");

        // Broadcast HH values to all executors
        Broadcast<Integer> bHH_B = spark.sparkContext().broadcast(HH_B, scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
        Broadcast<Integer> bHH_C = spark.sparkContext().broadcast(HH_C, scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        // J1: non-HH (B) and non-HH (C)
        Dataset<Row> R_J1 = R.filter(R.col("B").notEqual(bHH_B.value()));
        Dataset<Row> S_J1 = S.filter(S.col("B").notEqual(bHH_B.value())).filter(S.col("C").notEqual(bHH_C.value()));
        Dataset<Row> T_J1 = T.filter(T.col("C").notEqual(bHH_C.value()));

        // J2: HH (B) and non-HH (C) [cite: 382]
        Dataset<Row> R_J2 = R.filter(R.col("B").equalTo(bHH_B.value()));
        Dataset<Row> S_J2 = S.filter(S.col("B").equalTo(bHH_B.value())).filter(S.col("C").notEqual(bHH_C.value()));
        Dataset<Row> T_J2 = T_J1; // T_J1 is already non-HH on C

        // J4: non-HH (B) and HH (C) [cite: 379] (paper calls this J1, but it's our J4)
        Dataset<Row> R_J4 = R_J1; // R_J1 is already non-HH on B
        Dataset<Row> S_J4 = S.filter(S.col("B").notEqual(bHH_B.value())).filter(S.col("C").equalTo(bHH_C.value()));
        Dataset<Row> T_J4 = T.filter(T.col("C").equalTo(bHH_C.value()));

        // J5: HH (B) and HH (C) [cite: 594]
        Dataset<Row> R_J5 = R_J2; // R_J2 is already HH on B
        Dataset<Row> S_J5 = S.filter(S.col("B").equalTo(bHH_B.value())).filter(S.col("C").equalTo(bHH_C.value()));
        Dataset<Row> T_J5 = T_J4; // T_J4 is already HH on C

        // --- 3. Execute Residual Joins ---
        System.out.println("Executing residual joins...");

        // J1 (non-HH/non-HH) is a simple, standard join.
        long rj1_count = executeJ1_StandardJoin(R_J1, S_J1, T_J1);
        System.out.println("J1 (non-HH) result count: " + rj1_count);

        // J2, J4, J5 are skewed and require the SharesSkew logic.
        // We'll use k=100 total reducers for our HH joins.
        int k = 100;

        // Execute J2 (HH-B, non-HH-C)
        JavaRDD<Row> rddJ2 = executeJ2_SkewJoin(k, R_J2, S_J2, T_J2);
        System.out.println("J2 (HH-B) result count: " + rddJ2.count());

        // Execute J4 (non-HH-B, HH-C)
        JavaRDD<Row> rddJ4 = executeJ4_SkewJoin(k, R_J4, S_J4, T_J4);
        System.out.println("J4 (HH-C) result count: " + rddJ4.count());

        // Execute J5 (HH-B, HH-C)
        JavaRDD<Row> rddJ5 = executeJ5_SkewJoin(k, R_J5, S_J5, T_J5);
        System.out.println("J5 (HH-B, HH-C) result count: " + rddJ5.count());
        
        // --- 4. Union All Results ---
        System.out.println("Unioning results...");
        
        // Convert J1 Dataset to RDD to union
        StructType finalSchema = R_J1.schema()
                .add("E", DataTypes.IntegerType)
                .add("C", DataTypes.IntegerType)
                .add("D", DataTypes.IntegerType);
        
        JavaRDD<Row> rddJ1 = R_J1
            .join(S_J1, R_J1.col("B").equalTo(S_J1.col("B")))
            .join(T_J1, S_J1.col("C").equalTo(T_J1.col("C")))
            .select(R_J1.col("A"), R_J1.col("B"), S_J1.col("E"), S_J1.col("C"), T_J1.col("D"))
            .javaRDD();

        JavaRDD<Row> finalRDD = rddJ1.union(rddJ2).union(rddJ4).union(rddJ5);

        long finalCount = finalRDD.count();
        System.out.println("--- SHARESKEW FINAL COUNT: " + finalCount + " ---");

        // --- 5. For Comparison: Vanilla Spark Join ---
        System.out.println("Running standard 'vanilla' Spark join for comparison...");
        long startTime = System.nanoTime();
        
        Dataset<Row> vanillaJoin = R
                .join(S, R.col("B").equalTo(S.col("B")))
                .join(T, S.col("C").equalTo(T.col("C")));

        long vanillaCount = vanillaJoin.count();
        long endTime = System.nanoTime();
        
        System.out.println("--- VANILLA JOIN FINAL COUNT: " + vanillaCount + " ---");
        System.out.println("Vanilla join time: " + (endTime - startTime) / 1_000_000 + " ms");
        System.out.println("Check the Spark UI (localhost:4040) to see the difference in stage execution time!");

        // Uncomment to see the final (combined) data
        // spark.createDataFrame(finalRDD, finalSchema).show(50);
        
        spark.stop();
    }
    
    /**
     * Executes J1, the non-skewed join, using standard Spark SQL.
     */
    private static long executeJ1_StandardJoin(Dataset<Row> R, Dataset<Row> S, Dataset<Row> T) {
        return R.join(S, R.col("B").equalTo(S.col("B")))
                .join(T, S.col("C").equalTo(T.col("C")))
                .count();
    }

    /**
     * Executes J2 (HH on B, non-HH on C) using SharesSkew logic.
     */
    private static JavaRDD<Row> executeJ2_SkewJoin(int k, Dataset<Row> R, Dataset<Row> S, Dataset<Row> T) {
        // Cost Model for J2:
        // Generic Cost: r*c*d*e + s*a*d + t*a*b*e
        // Dominance: a=1 (by B), d=1 (by C), e=1 (by B,C) [cite: 619]
        // J2 constraints: B is HH -> b=1 [cite: 310]
        // Dominance revisited: C is not HH, so d is still dominated by C (d=1)[cite: 303].
        // B is HH, so a and e are *not* dominated by B.
        // E is also dominated by C (non-HH), so e=1.
        // A is *not* dominated by B (HH).
        // Cost = r*c + s*a + t*a [cite: 627]
        // Constraint: a * c = k
        // Solving r*c = s*a + t*a = a(s+t)
        // c = k/a => r*k/a = a(s+t) => a^2 = rk / (s+t) => a = sqrt(rk / (s+t))
        // c = k / a
        
        long r = Math.max(1, R.count());
        long s = Math.max(1, S.count());
        long t = Math.max(1, T.count());

        int shareA = (int) Math.round(Math.sqrt((r * k) / (double)(s + t)));
        shareA = Math.max(1, shareA);
        int shareC = k / shareA;
        shareC = Math.max(1, shareC);

        // Replicate R(A,B): has A, needs C
        JavaPairRDD<String, Row> rddR_J2 = R.javaRDD().flatMapToPair((PairFlatMapFunction<Row, String, Row>) row -> {
            List<Tuple2<String, Row>> emitted = new ArrayList<>();
            int hashA = Math.abs(Objects.hash(row.get(0)) % shareA);
            for (int c = 0; c < shareC; c++) {
                String key = hashA + "_" + c; // Key = (keyA, keyC)
                emitted.add(new Tuple2<>(key, row));
            }
            return emitted.iterator();
        });

        // Replicate S(B,E,C): has none of (A,C) as non-dominated. But has C.
        JavaPairRDD<String, Row> rddS_J2 = S.javaRDD().flatMapToPair((PairFlatMapFunction<Row, String, Row>) row -> {
            List<Tuple2<String, Row>> emitted = new ArrayList<>();
            int hashC = Math.abs(Objects.hash(row.get(2)) % shareC);
            for (int a = 0; a < shareA; a++) {
                String key = a + "_" + hashC; // Key = (keyA, keyC)
                emitted.add(new Tuple2<>(key, row));
            }
            return emitted.iterator();
        });

        // Replicate T(C,D): has C, needs A
        JavaPairRDD<String, Row> rddT_J2 = T.javaRDD().flatMapToPair((PairFlatMapFunction<Row, String, Row>) row -> {
            List<Tuple2<String, Row>> emitted = new ArrayList<>();
            int hashC = Math.abs(Objects.hash(row.get(0)) % shareC);
            for (int a = 0; a < shareA; a++) {
                String key = a + "_" + hashC; // Key = (keyA, keyC)
                emitted.add(new Tuple2<>(key, row));
            }
            return emitted.iterator();
        });
        
        // Co-group and join in the "reducer"
        return rddR_J2.cogroup(rddS_J2, rddT_J2).flatMap(cogroupedTuples());
    }
    
    /**
     * Executes J4 (non-HH on B, HH on C) using SharesSkew logic.
     */
    private static JavaRDD<Row> executeJ4_SkewJoin(int k, Dataset<Row> R, Dataset<Row> S, Dataset<Row> T) {
        // Cost Model for J4:
        // Generic Cost: r*c*d*e + s*a*d + t*a*b*e
        // J4 constraints: C is HH -> c=1 [cite: 310]
        // Dominance revisited[cite: 303]:
        // B is not HH, so a and e are dominated by B (a=1, e=1).
        // C is HH, so d is *not* dominated by C.
        // Cost = r*d + s*d + t*b [cite: 638]
        // Constraint: b * d = k
        // Solving r*d + s*d = t*b => d(r+s) = t*b
        // b = k/d => d(r+s) = t*k/d => d^2 = tk / (r+s) => d = sqrt(tk / (r+s))
        // b = k / d
        
        long r = Math.max(1, R.count());
        long s = Math.max(1, S.count());
        long t = Math.max(1, T.count());

        int shareD = (int) Math.round(Math.sqrt((t * k) / (double)(r + s)));
        shareD = Math.max(1, shareD);
        int shareB = k / shareD;
        shareB = Math.max(1, shareB);

        // Replicate R(A,B): has B, needs D
        JavaPairRDD<String, Row> rddR_J4 = R.javaRDD().flatMapToPair((PairFlatMapFunction<Row, String, Row>) row -> {
            List<Tuple2<String, Row>> emitted = new ArrayList<>();
            int hashB = Math.abs(Objects.hash(row.get(1)) % shareB);
            for (int d = 0; d < shareD; d++) {
                String key = hashB + "_" + d; // Key = (keyB, keyD)
                emitted.add(new Tuple2<>(key, row));
            }
            return emitted.iterator();
        });

        // Replicate S(B,E,C): has B, needs D
        JavaPairRDD<String, Row> rddS_J4 = S.javaRDD().flatMapToPair((PairFlatMapFunction<Row, String, Row>) row -> {
            List<Tuple2<String, Row>> emitted = new ArrayList<>();
            int hashB = Math.abs(Objects.hash(row.get(0)) % shareB);
            for (int d = 0; d < shareD; d++) {
                String key = hashB + "_" + d; // Key = (keyB, keyD)
                emitted.add(new Tuple2<>(key, row));
            }
            return emitted.iterator();
        });

        // Replicate T(C,D): has D, needs B
        JavaPairRDD<String, Row> rddT_J4 = T.javaRDD().flatMapToPair((PairFlatMapFunction<Row, String, Row>) row -> {
            List<Tuple2<String, Row>> emitted = new ArrayList<>();
            int hashD = Math.abs(Objects.hash(row.get(1)) % shareD);
            for (int b = 0; b < shareB; b++) {
                String key = b + "_" + hashD; // Key = (keyB, keyD)
                emitted.add(new Tuple2<>(key, row));
            }
            return emitted.iterator();
        });
        
        // Co-group and join in the "reducer"
        return rddR_J4.cogroup(rddS_J4, rddT_J4).flatMap(cogroupedTuples());
    }

    /**
     * Executes J5 (HH on B, HH on C) using SharesSkew logic.
     * This is the most complex case, as described in[cite: 639].
     */
    private static JavaRDD<Row> executeJ5_SkewJoin(int k, Dataset<Row> R, Dataset<Row> S, Dataset<Row> T) {
        // --- 3a. Calculate Shares for J5 ---
        // Cost Model for J5 (HH-B, HH-C):
        // Generic Cost: r*c*d*e + s*a*d + t*a*b*e
        // J5 constraints: B is HH -> b=1. C is HH -> c=1[cite: 310].
        // Dominance revisited[cite: 303]:
        // B is HH, so a and e are *not* dominated by B.
        // C is HH, so d and e are *not* dominated by C.
        // Result: No attributes are dominated. All shares (a,d,e) are > 1.
        // Cost = r*d*e + s*a*d + t*a*e [cite: 639]
        // Constraint: a * d * e = k

        // To find the minimum, we set the terms equal, as shown in [cite: 639-640]:
        // r*d*e = s*a*d  =>  r*e = s*a
        // s*a*d = t*a*e  =>  s*d = t*e
        // r*d*e = t*a*e  =>  r*d = t*a

        // Get the (small) sizes of the HH residual relations
        long r = Math.max(1, R.count());
        long s = Math.max(1, S.count());
        long t = Math.max(1, T.count());

        // Solve for a, d, e in terms of k:
        // From r*e = s*a => e = s*a / r
        // From r*d = t*a => d = t*a / r
        // Sub into constraint: a * (t*a / r) * (s*a / r) = k
        // a^3 * (s*t / r^2) = k  =>  a^3 = (k * r^2) / (s*t)
        int shareA = (int) Math.round(Math.cbrt((k * r * r) / (double) (s * t)));
        shareA = Math.max(1, shareA);

        // d^3 = (k * t^2) / (r*s)
        int shareD = (int) Math.round(Math.cbrt((k * t * t) / (double) (r * s)));
        shareD = Math.max(1, shareD);

        // e^3 = (k * s^2) / (r*t)
        int shareE = (int) Math.round(Math.cbrt((k * s * s) / (double) (r * t)));
        shareE = Math.max(1, shareE);
        
        // This is a discrete approximation. We adjust 'k' or shares
        // to ensure the product is close. For this demo, we'll use these values.
        
        System.out.println(String.format("J5 (HH) Shares (r=%d, s=%d, t=%d, k=%d): a=%d, d=%d, e=%d",
                r, s, t, k, shareA, shareD, shareE));
                
        final int fShareA = shareA;
        final int fShareD = shareD;
        final int fShareE = shareE;

        // --- 3b. Replicate Tuples (Map Phase) ---
        // This is the core "Shares" replication [cite: 199-201]

        // R(A,B) has A. Must be replicated for D and E. [cite: 200]
        JavaPairRDD<ReducerKey, Row> rddR_J5 = R.javaRDD().flatMapToPair(
                (PairFlatMapFunction<Row, ReducerKey, Row>) row -> {
                    List<Tuple2<ReducerKey, Row>> emitted = new ArrayList<>();
                    // Hash the attribute we have
                    int hashA = Math.abs(Objects.hash(row.get(0)) % fShareA);
                    
                    // Replicate for attributes we don't have
                    for (int d = 0; d < fShareD; d++) {
                        for (int e = 0; e < fShareE; e++) {
                            emitted.add(new Tuple2<>(new ReducerKey(hashA, d, e), row));
                        }
                    }
                    return emitted.iterator();
                });

        // S(B,E,C) has E. Must be replicated for A and D.
        JavaPairRDD<ReducerKey, Row> rddS_J5 = S.javaRDD().flatMapToPair(
                (PairFlatMapFunction<Row, ReducerKey, Row>) row -> {
                    List<Tuple2<ReducerKey, Row>> emitted = new ArrayList<>();
                    int hashE = Math.abs(Objects.hash(row.get(1)) % fShareE); // E is at index 1
                    
                    for (int a = 0; a < fShareA; a++) {
                        for (int d = 0; d < fShareD; d++) {
                            emitted.add(new Tuple2<>(new ReducerKey(a, d, hashE), row));
                        }
                    }
                    return emitted.iterator();
                });

        // T(C,D) has D. Must be replicated for A and E.
        JavaPairRDD<ReducerKey, Row> rddT_J5 = T.javaRDD().flatMapToPair(
                (PairFlatMapFunction<Row, ReducerKey, Row>) row -> {
                    List<Tuple2<ReducerKey, Row>> emitted = new ArrayList<>();
                    int hashD = Math.abs(Objects.hash(row.get(1)) % fShareD); // D is at index 1
                    
                    for (int a = 0; a < fShareA; a++) {
                        for (int e = 0; e < fShareE; e++) {
                            emitted.add(new Tuple2<>(new ReducerKey(a, hashD, e), row));
                        }
                    }
                    return emitted.iterator();
                });

        // --- 3c. Group and Join (Reduce Phase) ---
        // We use cogroup to bring all tuples with the same ReducerKey to one executor
        JavaPairRDD<ReducerKey, Tuple3<Iterable<Row>, Iterable<Row>, Iterable<Row>>> cogrouped =
                rddR_J5.cogroup(rddS_J5, rddT_J5);

        // Now we flatMap, where each task is a "reducer"
        return cogrouped.flatMap(cogroupedTuples());
    }
    
    /**
     * This function represents the "Reducer" task.
     * It takes the cogrouped tuples for one ReducerKey and performs the
     * in-memory join.
     */
    private static FlatMapFunction<
            Tuple2<String, Tuple3<Iterable<Row>, Iterable<Row>, Iterable<Row>>>, 
            Row
        > cogroupedTuples() {
        
        return t -> {
            Iterable<Row> rTuplesIter = t._2._1();
            Iterable<Row> sTuplesIter = t._2._2();
            Iterable<Row> tTuplesIter = t._2._3();

            // Store in memory for nested loop join
            List<Row> rTuples = new ArrayList<>();
            rTuplesIter.forEach(rTuples::add);
            
            List<Row> sTuples = new ArrayList<>();
            sTuplesIter.forEach(sTuples::add);

            List<Row> tTuples = new ArrayList<>();
            tTuplesIter.forEach(tTuples::add);

            List<Row> joinedRows = new ArrayList<>();
            
            // If any list is empty, this reducer produces no results
            if (rTuples.isEmpty() || sTuples.isEmpty() || tTuples.isEmpty()) {
                return Collections.emptyIterator();
            }

            // Perform the multi-way join in memory
            for (Row r : rTuples) {
                int r_B = r.getInt(1);
                for (Row s : sTuples) {
                    int s_B = s.getInt(0);
                    // R(B) == S(B)
                    if (r_B == s_B) {
                        int s_C = s.getInt(2);
                        for (Row t : tTuples) {
                            int t_C = t.getInt(0);
                            // S(C) == T(C)
                            if (s_C == t_C) {
                                // Emit the final joined tuple
                                joinedRows.add(RowFactory.create(
                                        r.get(0), // A
                                        r.get(1), // B
                                        s.get(1), // E
                                        s.get(2), // C
                                        t.get(1)  // D
                                ));
                            }
                        }
                    }
                }
            }
            return joinedRows.iterator();
        };
    }

    /**
     * Generates sample data for R(A,B), S(B,E,C), T(C,D) with skew.
     */
    private static Map<String, Dataset<Row>> generateSkewedData(SparkSession spark) {
        StructType schemaR = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("A", DataTypes.IntegerType, true),
                DataTypes.createStructField("B", DataTypes.IntegerType, true)
        });
        StructType schemaS = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("B", DataTypes.IntegerType, true),
                DataTypes.createStructField("E", DataTypes.IntegerType, true),
                DataTypes.createStructField("C", DataTypes.IntegerType, true)
        });
        StructType schemaT = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("C", DataTypes.IntegerType, true),
                DataTypes.createStructField("D", DataTypes.IntegerType, true)
        });

        List<Row> dataR = new ArrayList<>();
        List<Row> dataS = new ArrayList<>();
        List<Row> dataT = new ArrayList<>();

        int nonHHCount = 10000;
        int hhCount = 100000; // 10x skew

        // --- Non-HH Data --- (B != 1, C != 10)
        for (int i = 0; i < nonHHCount; i++) {
            int b_val = i + 2; // (2, 3, ...)
            int c_val = i + 11; // (11, 12, ...)
            dataR.add(RowFactory.create(i, b_val));
            dataS.add(RowFactory.create(b_val, i * 10, c_val));
            dataT.add(RowFactory.create(c_val, i * 20));
        }

        // --- HH Data ---
        // J5 (HH-B, HH-C): B=1, C=10
        for (int i = 0; i < hhCount; i++) {
            dataR.add(RowFactory.create(i + 100000, HH_B));
            dataS.add(RowFactory.create(HH_B, i + 200000, HH_C));
            dataT.add(RowFactory.create(HH_C, i + 300000));
        }
        
        // J2 (HH-B, non-HH-C): B=1, C=12
        int c_nonHH = 12;
        for (int i = 0; i < hhCount; i++) {
            dataR.add(RowFactory.create(i + 400000, HH_B)); // R(A, 1)
            dataS.add(RowFactory.create(HH_B, i + 500000, c_nonHH)); // S(1, E, 12)
            // We need matching T tuples
            dataT.add(RowFactory.create(c_nonHH, i + 600000)); // T(12, D)
        }
        
        // J4 (non-HH-B, HH-C): B=3, C=10
        int b_nonHH = 3;
        for (int i = 0; i < hhCount; i++) {
            dataR.add(RowFactory.create(i + 700000, b_nonHH)); // R(A, 3)
            dataS.add(RowFactory.create(b_nonHH, i + 800000, HH_C)); // S(3, E, 10)
            // We need matching T tuples
            dataT.add(RowFactory.create(HH_C, i + 900000)); // T(10, D)
        }
        
        Map<String, Dataset<Row>> datasets = new HashMap<>();
        datasets.put("R", spark.createDataFrame(dataR, schemaR));
        datasets.put("S", spark.createDataFrame(dataS, schemaS));
        datasets.put("T", spark.createDataFrame(dataT, schemaT));
        return datasets;
    }
}