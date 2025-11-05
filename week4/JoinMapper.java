import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Get the name of the file the input is coming from
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String line = value.toString();
        String[] fields = line.split(",");

        if (fields.length < 2) {
            return; // Skip malformed lines
        }

        if (fileName.startsWith("customers")) {
            // Input is from customers.csv: CustomerID,Name,City
            String customerId = fields[0];
            String customerInfo = "CUST#" + fields[1] + "," + fields[2];
            
            outKey.set(customerId);
            outValue.set(customerInfo);
            context.write(outKey, outValue);

        } else if (fileName.startsWith("orders")) {
            // Input is from orders.csv: OrderID,CustomerID,Product
            String customerId = fields[1];
            String orderInfo = "ORD#" + fields[0] + "," + fields[2];
            
            outKey.set(customerId);
            outValue.set(orderInfo);
            context.write(outKey, outValue);
        }
    }
}