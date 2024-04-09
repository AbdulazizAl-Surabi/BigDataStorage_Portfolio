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

// Hauptklasse für den zweiten MapReduce-Job, der Kunden in Gruppen einteilt basierend auf erworbenen Punkten
public class CustomerGroupPointsJob {

    // Benutzerdefinierte Writable-Klasse für die Übertragung von Punktedaten zwischen Mapper und Reducer
    public static class PointsTuple implements Writable {
        private int accumulatedPoints; // Gesamtzahl der erworbenen Punkte
        private int redeemedPoints; // Gesamtzahl der eingelösten Punkte

        // Default-Konstruktor ist erforderlich für die interne Deserialisierung
        public PointsTuple() {
        }

        // Konstruktor für die Initialisierung mit Werten
        public PointsTuple(int accumulated, int redeemed) {
            this.accumulatedPoints = accumulated;
            this.redeemedPoints = redeemed;
        }

        // Getter und Setter für die Punkte
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

        // Methoden für die Serialisierung und Deserialisierung der Objekte
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

        // Für eine lesbare Ausgabe der Punkte
        @Override
        public String toString() {
            return accumulatedPoints + "\t" + redeemedPoints;
        }

        // Hilfsmethode zur einfachen Setzung beider Werte
        public void set(int accumulatedPoints, int redeemedPoints) {
            this.accumulatedPoints = accumulatedPoints;
            this.redeemedPoints = redeemedPoints;
        }
    }

    // Mapper, der Eingabedaten verarbeitet und jedem Kunden basierend auf Punkten eine Gruppe zuweist
    public static class CustomerGroupMapper extends Mapper<Object, Text, Text, PointsTuple> {
        private Text group = new Text(); // Gruppen-ID
        private PointsTuple pointsTuple = new PointsTuple(); // Container für Punktedaten

        // Verarbeitet jede Zeile der Eingabedaten
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length >= 3) {
                int accumulatedPoints = Integer.parseInt(parts[1]);
                int redeemedPoints = Integer.parseInt(parts[2]);
                pointsTuple.set(accumulatedPoints, redeemedPoints);

                // Zuweisung der Gruppen-ID basierend auf der Punktzahl
                String groupId = getGroupId(accumulatedPoints);
                group.set(groupId);
                // Schreibt Gruppen-ID und Punktedaten für die Reduzierung
                context.write(group, pointsTuple);
            }
        }
        
        // Bestimmt die Gruppen-ID basierend auf der Punktzahl
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

    // Reducer, der die Ergebnisse pro Gruppe zusammenfasst und statistische Informationen berechnet
    public static class CustomerGroupReducer extends Reducer<Text, PointsTuple, Text, Text> {
        // Aggregiert Daten für jede Gruppe
        public void reduce(Text key, Iterable<PointsTuple> values, Context context) throws IOException, InterruptedException {
            int customerCount = 0; // Anzahl der Kunden in der Gruppe
            int totalAccumulated = 0; // Gesamtzahl der erworbenen Punkte
            int totalRedeemed = 0; // Gesamtzahl der eingelöstenPunkte. 
            for (PointsTuple val : values) {
            // Zusammenfassung der Gruppendaten und Berechnung des Prozentsatzes der eingelösten Punkte
                totalAccumulated += val.getAccumulatedPoints();
                totalRedeemed += val.getRedeemedPoints();
                customerCount++;
            }

            // Zusammenfassung der Gruppendaten und Berechnung des Prozentsatzes der eingelösten Punkte
            double redeemedPercentage = (totalAccumulated > 0) ? (100.0 * totalRedeemed / totalAccumulated) : 0.0;
            String summary = String.format("AnzahlKunden: %d, SummePunkte: %d, ProzentEingeloest: %.2f%%",
                    customerCount, totalAccumulated, redeemedPercentage);
                    
            // Schreiben der Gruppenzusammenfassung als Ausgabe
            context.write(key, new Text(summary));
        }
    }

    // Hauptmethode zur Konfiguration und Start des Jobs
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Group Points");
        job.setJarByClass(CustomerGroupPointsJob.class); // Setzt die Jar, in der der Job ausgeführt wird
        job.setMapperClass(CustomerGroupMapper.class); // Setzt die Mapper-Klasse
        job.setReducerClass(CustomerGroupReducer.class); // Setzt die Reducer-Klasse
        job.setOutputKeyClass(Text.class); // Setzt den Typ des Ausgabe-Schlüssels
        job.setOutputValueClass(PointsTuple.class); // Setzt den Typ des Ausgabe-Wertes

        // Setzt die Pfade für Eingabe- und Ausgabedaten
        FileInputFormat.addInputPath(job, new Path("intermediate-output")); 
        FileOutputFormat.setOutputPath(job, new Path("final-output"));

        // Startet den Job und wartet auf dessen Beendigung
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
