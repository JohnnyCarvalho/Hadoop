package TDE_2.question01;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

/**
 * Questions
 * 01 - Número de transações envolvendo o Brasil.
 * Resultado esperado: 184748
 * @author Johnny Carvalho
 * @version 1.0
 * @since 2025-04-15
 */
public class Question01 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        Configuration config = new Configuration();

        final Path input = new Path("in/data_tde_2.csv");
        final Path output = new Path("output/question01");

        final Job job = Job.getInstance(config, "Transações envolvendo o Brasil");

        job.setJarByClass(Question01.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        final boolean success = job.waitForCompletion(true);
        System.out.println("Job finalizado com sucesso? " + success);
        System.exit(success ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(final LongWritable key, final Text value, final Context context)
                throws IOException, InterruptedException {

            final String line = value.toString();

            if (line.toLowerCase().contains("country_or_area")) {
                return;
            }

            try {
                final String[] columns = line.split(";");
                final String country = columns[0].trim();
                if (country.equalsIgnoreCase("Brazil")) {
                    context.write(new Text("BRAZIL"), new IntWritable(1));
                }
            } catch (Exception e) {
                System.err.println("Erro ao processar linha: " + line);
                e.printStackTrace();
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
