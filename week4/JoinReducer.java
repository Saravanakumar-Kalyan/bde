import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    private List<String> customerList = new ArrayList<>();
    private List<String> orderList = new ArrayList<>();
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
        customerList.clear();
        orderList.clear();

        for (Text val : values) {
            String valueStr = val.toString();
            if (valueStr.startsWith("CUST#")) {
                customerList.add(valueStr.substring(5));
            } else if (valueStr.startsWith("ORD#")) {
                orderList.add(valueStr.substring(4));
            }
        }

        if (!customerList.isEmpty() && !orderList.isEmpty()) {
            for (String customer : customerList) {
                for (String order : orderList) {
                    result.set(customer + "," + order);
                    context.write(key, result);
                }
            }
        }
    }
}