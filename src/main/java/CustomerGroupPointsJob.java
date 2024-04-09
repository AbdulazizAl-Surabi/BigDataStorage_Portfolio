import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomerGroupPointsJob {

    public static class PointsTuple implements Writable {
        private int accumulatedPoints;
        private int redeemedPoints;

        public PointsTuple() {
        }

        public PointsTuple(int accumulated, int redeemed) {
            this.accumulatedPoints = accumulated;
            this.redeemedPoints = redeemed;
        }

        public int getAccumulatedPoints() {
            return accumulatedPoints;
        }

        public void setAccumulatedPoints(int accumulatedPoints) {
            this.accumulatedPoints = accumulatedPoints;
        }

        public int getRedeemedPoints() {
            return redeemedPoints;
        }

        public void setRedeemedPoints(int redeemedPoints) {
            this.redeemedPoints = redeemedPoints;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(accumulatedPoints);
            out.writeInt(redeemedPoints);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            accumulatedPoints = in.readInt();
            redeemedPoints = in.readInt();
        }

        @Override
        public String toString() {
            return accumulatedPoints + "\t" + redeemedPoints;
        }

        public void set(int accumulatedPoints, int redeemedPoints) {
            this.accumulatedPoints = accumulatedPoints;
            this.redeemedPoints = redeemedPoints;
        }

    }

    public static class CustomerGroupMapper extends Mapper<Object, Text, Text, PointsTuple> {
        private Text group = new Text();
        private PointsTuple pointsTuple = new PointsTuple();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length >= 3) {
                int accumulatedPoints = Integer.parseInt(parts[1]);
                int redeemedPoints = Integer.parseInt(parts[2]);
                pointsTuple.set(accumulatedPoints, redeemedPoints);

                String groupId = getGroupId(accumulatedPoints);
                group.set(groupId);
                context.write(group, pointsTuple);
            }
        }
        
        private String getGroupId(int points) {
            if (points <= 2000) {
                return "Gruppe1_Bis2000Punkte";
            } else if (points <= 10000) {
                return "Gruppe2_2001Bis10000Punkte";
            } else {
                return "Gruppe3_Ueber10000Punkte";
            }
        }
    }

    public static class CustomerGroupReducer extends Reducer<Text, PointsTuple, Text, Text> {
        public void reduce(Text key, Iterable<PointsTuple> values, Context context) throws IOException, InterruptedException {
            int customerCount = 0;
            int totalAccumulated = 0;
            int totalRedeemed = 0;

            for (PointsTuple val : values) {
                totalAccumulated += val.getAccumulatedPoints();
                totalRedeemed += val.getRedeemedPoints();
                customerCount++;
            }

            double redeemedPercentage = (totalAccumulated > 0) ? (100.0 * totalRedeemed / totalAccumulated) : 0.0;
            String summary = String.format("AnzahlKunden: %d, SummePunkte: %d, ProzentEingeloest: %.2f%%",
                    customerCount, totalAccumulated, redeemedPercentage);

            context.write(key, new Text(summary));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Group Points");
        job.setJarByClass(CustomerGroupPointsJob.class);
        job.setMapperClass(CustomerGroupMapper.class);
        job.setReducerClass(CustomerGroupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PointsTuple.class);

        FileInputFormat.addInputPath(job, new Path("intermediate-output"));
        FileOutputFormat.setOutputPath(job, new Path("final-output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
