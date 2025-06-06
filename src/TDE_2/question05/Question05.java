package TDE_2.question05;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * Questão 05 - Valor médio das transações por ano somente no Brasil.
 * Resultado esperado: lista com o total de transações agrupadas por categoria.
 * @author Johnny Carvalho
 */
public class Question05 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        final Configuration config = new Configuration();

        final Path input = new Path("in/data_tde_2.csv");
        final Path output = new Path("output/question05");

        final Job job = Job.getInstance(config, "Quantidade de transações envolvendo o Brasil");

        job.setJarByClass(Question05.class);
        job.setMapperClass(Question05.MapAverageTransactions.class);
        job.setReducerClass(Question05.ReduceAverageTransactions.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(CustomQuestion05.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        final boolean success = job.waitForCompletion(true);
        System.out.println("Job finalizado com sucesso? " + success);
        System.exit(success ? 0 : 1);
    }

    /**
     * Class with the map method to process the input data
     * where the key is the country and the value is the quantity of transactions.
     */
    public static class MapAverageTransactions extends Mapper<LongWritable, Text, IntWritable, CustomQuestion05> {
        private final IntWritable yearKey = new IntWritable();

        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(";");
            final String country = columns[0].trim();
            final String yearStr = columns[1].trim();
            final String valueStr = columns[5].trim();
            final String line = value.toString();
            if (line.toLowerCase().contains("country_or_area")) {
                return;
            }

            if (!country.equals("Brazil")) {
                return;
            }

            try {
                final int year = Integer.parseInt(yearStr);
                final double transactionValue = Double.parseDouble(valueStr.replace(",", "."));
                yearKey.set(year);
                context.write(yearKey, new CustomQuestion05(transactionValue, 1));
            } catch (NumberFormatException e) {
                System.out.println("Erro ao converter valor: " + e.getMessage());
            }
        }
    }

    public static class ReduceAverageTransactions extends Reducer<IntWritable, CustomQuestion05, IntWritable, Text> {
        public void reduce(final IntWritable key, final Iterable<CustomQuestion05> values, final Context context)
                throws IOException, InterruptedException {

            double somaTotal = 0;
            int totalCount = 0;

            for (CustomQuestion05 val : values) {
                somaTotal += val.getSoma();
                totalCount += val.getContagem();
            }

            if (totalCount > 0) {
                final double media = somaTotal / totalCount;
                final String mediaStr = String.format("%.2f", media);
                context.write(key, new Text(mediaStr));
            }
        }
    }
}
