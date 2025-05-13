import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ValorPorDominio {

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split(",");

            if (parts.length == 4 && !parts[0].equals("Domain")) {
                String domain = parts[0].trim();
                try {
                    double val = Double.parseDouble(parts[2].trim());
                    context.write(new Text(domain), new DoubleWritable(val));
                } catch (NumberFormatException e) {
                    // Ignorar registros con errores
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }

            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Valor Total por Dominio");

        job.setJarByClass(ValorPorDominio.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // Entrada: CSV en HDFS
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Salida: directorio HDFS

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}