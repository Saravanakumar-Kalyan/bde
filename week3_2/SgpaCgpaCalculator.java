import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


class SubjectDataWritable implements Writable {
    private Text semester;
    private Text grade;
    private IntWritable credits;

    public SubjectDataWritable() {
        this.semester = new Text();
        this.grade = new Text();
        this.credits = new IntWritable();
    }

    public SubjectDataWritable(String semester, String grade, int credits) {
        this.semester = new Text(semester);
        this.grade = new Text(grade);
        this.credits = new IntWritable(credits);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        semester.write(out);
        grade.write(out);
        credits.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        semester.readFields(in);
        grade.readFields(in);
        credits.readFields(in);
    }

    public Text getSemester() {
        return semester;
    }

    public Text getGrade() {
        return grade;
    }

    public IntWritable getCredits() {
        return credits;
    }

    @Override
    public String toString() {
        return "Semester: " + semester.toString() + ", Grade: " + grade.toString() + ", Credits: " + credits.get();
    }
}


class SgpaCgpaMapper extends Mapper<Object, Text, Text, SubjectDataWritable> {

    private Text studentId = new Text();
    private SubjectDataWritable subjectData = new SubjectDataWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");

        if (parts.length != 6) {
            return;
        }

        try {
            String sId = parts[1].trim();
            String sGrade = parts[2].trim();
            int sCredits = Integer.parseInt(parts[5].trim());
            String sSemester = parts[4].trim();

            studentId.set(sId);
            subjectData = new SubjectDataWritable(sSemester, sGrade, sCredits);

            context.write(studentId, subjectData);
        } catch (NumberFormatException e) {
            System.err.println("Skipping line due to invalid number format: " + line);
        }
    }
}

class SgpaCgpaReducer extends Reducer<Text, SubjectDataWritable, Text, Text> {

    class DoubleHolder {
        public double value = 0.0;
    }

    private double getGradePoint(String grade) {
        switch (grade.toUpperCase()) {
            case "EX":
                return 10.0;
            case "A":
                return 9.0;
            case "B":
                return 8.0;
            case "C":
                return 7.0;
            case "D":
                return 6.0;
            case "P":
                return 5.0;
            default:
                return 0.0;
        }
    }

    @Override
    public void reduce(Text key, Iterable<SubjectDataWritable> values, Context context)
            throws IOException, InterruptedException {

        Map<String, Double> semesterGradePoints = new HashMap<>();
        Map<String, Double> semesterCredits = new HashMap<>();

        for (SubjectDataWritable subject : values) {
            String semester = subject.getSemester().toString();
            String grade = subject.getGrade().toString();
            int credits = subject.getCredits().get();

            double gradePoint = getGradePoint(grade);

            semesterGradePoints.put(semester,
                    semesterGradePoints.getOrDefault(semester, 0.0) + (gradePoint * credits));
            semesterCredits.put(semester,
                    semesterCredits.getOrDefault(semester, 0.0) + credits);
        }

        StringBuilder outputValue = new StringBuilder();
        
        DoubleHolder totalCumulativePoints = new DoubleHolder();
        DoubleHolder totalCumulativeCredits = new DoubleHolder();

        // outputValue.append("student_id:").append(key.toString());
        semesterGradePoints.keySet().stream().sorted().forEach(semester -> {
            double totalPoints = semesterGradePoints.get(semester);
            double totalCredits = semesterCredits.get(semester);
            double sgpa = 0.0;

            if (totalCredits > 0) {
                sgpa = totalPoints / totalCredits;
            }

            outputValue.append(",sgpa_").append(semester).append(":").append(String.format("%.2f", sgpa));

            totalCumulativePoints.value += totalPoints;
            totalCumulativeCredits.value += totalCredits;
        });

        double cgpa = 0.0;
        if (totalCumulativeCredits.value > 0) {
            cgpa = totalCumulativePoints.value / totalCumulativeCredits.value;
        }

        String finalOutput = outputValue.toString().substring(1) + ",cgpa:" + String.format("%.2f", cgpa);

        context.write(key, new Text(finalOutput));
    }
}


public class SgpaCgpaCalculator {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2) {
            System.err.println("Usage: SgpaCgpaCalculator <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "SGPA and CGPA Calculator");
        job.setJarByClass(SgpaCgpaCalculator.class);

        job.setMapperClass(SgpaCgpaMapper.class);
        job.setReducerClass(SgpaCgpaReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SubjectDataWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
