import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

// Hauptklasse für den MapReduce-Job
public class CustomerPointsJob {

    // Eine benutzerdefinierte Writable-Klasse, die ein Tupel von Punkten speichert
    public static class PointsTuple implements Writable {
        private int accumulatedPoints;
        private int redeemedPoints;

        // Default-Konstruktor für Reflexion während des MapReduce-Prozesses
        public PointsTuple() {
        }

        // Konstruktor, um ein Tupel aus angesammelten und eingelösten Punkten zu erstellen
        public PointsTuple(int accumulated, int redeemed) {
            this.accumulatedPoints = accumulated;
            this.redeemedPoints = redeemed;
        }

        // Schreiben der Daten in einen Ausgabestrom für die Serialisierung
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(accumulatedPoints);
            out.writeInt(redeemedPoints);
        }

        // Lesen der Daten aus einem Eingabestrom für die Deserialisierung
        @Override
        public void readFields(DataInput in) throws IOException {
            accumulatedPoints = in.readInt();
            redeemedPoints = in.readInt();
        }

        // Überschreibung der toString-Methode für die Ausgabe
        @Override
        public String toString() {
            return accumulatedPoints + "\t" + redeemedPoints;
        }

        // Getter- und Setter-Methoden
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
    }

    // Der Mapper, der die Eingabezeilen verarbeitet
    public static class CustomerPointsMapper extends Mapper<Object, Text, Text, PointsTuple> {
        private Text customerId = new Text();
        private PointsTuple pointsTuple = new PointsTuple();

        // Die Map-Methode, die für jede Eingabezeile aufgerufen wird
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Kopfzeile überspringen
            if (value.toString().contains("Customer")) {
                return;
            }

            // Aufspaltung der CSV-Zeile in ihre Komponenten
            String[] parts = value.toString().split(",");
            if (parts.length > 8) {
                customerId.set(parts[0]);
                // Runden der Fließkommazahlen und Umwandlung in Ganzzahlen
                int accumulatedPoints = (int) Math.round(Double.parseDouble(parts[7].trim()));
                int redeemedPoints = (int) Math.round(Double.parseDouble(parts[8].trim()));

                // Setzen der Werte im Tupel
                pointsTuple.setAccumulatedPoints(accumulatedPoints);
                pointsTuple.setRedeemedPoints(redeemedPoints);
                // Schreiben des Outputs für die Reduzierung
                context.write(customerId, pointsTuple);
            }
        }
    }

    // Der Reducer, der die Ergebnisse aus dem Mapper zusammenfasst
    public static class CustomerPointsReducer extends Reducer<Text, PointsTuple, Text, PointsTuple> {
        private PointsTuple result = new PointsTuple();

        // Die Reduce-Methode, die für jede Kunden-ID aufgerufen wird
        public void reduce(Text key, Iterable<PointsTuple> values, Context context) throws IOException, InterruptedException {
            int totalAccumulated = 0;
            int totalRedeemed = 0;

            // Zusammenfassen der Punkte für jeden Kunden
            for (PointsTuple val : values) {
                totalAccumulated += val.getAccumulatedPoints();
                totalRedeemed += val.getRedeemedPoints();
            }

            // Setzen des Gesamtergebnisses und Schreiben des Outputs
            result.setAccumulatedPoints(totalAccumulated);
            result.setRedeemedPoints(totalRedeemed);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Points Calculation");
        job.setJarByClass(CustomerPointsJob.class);
        job.setMapperClass(CustomerPointsMapper.class);
        job.setReducerClass(CustomerPointsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PointsTuple.class);

        FileInputFormat.addInputPath(job, new Path("input")); 
        FileOutputFormat.setOutputPath(job, new Path("intermediate-output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

