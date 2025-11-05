import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class RangePartitioner extends Partitioner<Text, Text> implements Configurable {

    private List<Integer> splitPoints = new ArrayList<>();
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        try {
            URI[] cacheFiles = org.apache.hadoop.mapreduce.Job.getCacheFiles(conf);
        
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path partitionFilePath = new Path(cacheFiles[0].getPath());
                readSplitPoints(partitionFilePath.getName());
            }
        } catch (IOException e) {
            System.err.println("Error reading partition file from cache: " + e.getMessage());
        }
    }

    private void readSplitPoints(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                splitPoints.add(Integer.parseInt(line.trim()));
            }
        }
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        try {
            int customerId = Integer.parseInt(key.toString());
            for (int i = 0; i < splitPoints.size(); i++) {
                if (customerId < splitPoints.get(i)) {
                    return i;
                }
            }
            return splitPoints.size();
        } catch (NumberFormatException e) {
            return key.toString().hashCode() % numPartitions;
        }
    }
}