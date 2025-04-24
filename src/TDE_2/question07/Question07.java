package TDE_2.question07;

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
 * Questão 07 - Valor médio das transações por ano, considerando somente as transações do
 * tipo exportação (Export) realizadas no Brasil.
 * Resultado esperado: lista com o total de transações agrupadas por categoria.
 * @author Johnny Carvalho.
 */
public class Question07 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();

        final Configuration config = new Configuration();

        final Path input = new Path("in/data_tde_2.csv");
        final Path output = new Path("output/question07");

        final Job job1 = Job.getInstance(config, "Valor medio ano exportação envolvendo o Brasil");

        job1.setJarByClass(Question07.class);
        job1.setMapperClass(MapAverageTransactions.class);
        job1.setReducerClass(ReduceAverageTransactions.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(CustomQuestion07.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, output);

        final boolean success = job1.waitForCompletion(true);
        System.out.println("Job finalizado com sucesso? " + success);
        System.exit(success ? 0 : 1);
    }

    /**
     * Class with the map method to process the input data.
     * @see Mapper
     * @see LongWritable
     * @see Text
     * @see IntWritable
     * @see CustomQuestion07
     * @see IOException
     * @see InterruptedException
     * @see Context
     * @see String
     */
    public static class MapAverageTransactions extends Mapper<LongWritable, Text, IntWritable, CustomQuestion07> {
        private final IntWritable yearKey = new IntWritable();

        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] columns = value.toString().split(";");
            final String country = columns[0].trim();
            final String yearStr = columns[1].trim();
            final String valueStr = columns[5].trim();
            final String flow = columns[4].trim();
            final String line = value.toString();
            if (line.toLowerCase().contains("country_or_area")) {
                return;
            }

            if (!country.equals("Brazil") && !flow.equalsIgnoreCase("export")) {
                return;
            }

            try {
                final int year = Integer.parseInt(yearStr);
                final double transactionValue = Double.parseDouble(valueStr.replace(",", "."));
                yearKey.set(year);
                context.write(yearKey, new CustomQuestion07(transactionValue, 1));
            } catch (NumberFormatException e) {
                System.out.println("Erro ao converter valor: " + e.getMessage());
            }
        }
    }

    /**
     * Class with the reduce method to process the intermediate data.
     * @see Reducer
     * @see IntWritable
     * @see CustomQuestion07
     * @see Text
     * @see IOException
     * @see InterruptedException
     * @see Context
     */
    public static class ReduceAverageTransactions extends Reducer<IntWritable, CustomQuestion07, IntWritable, Text> {
        public void reduce(final IntWritable key, final Iterable<CustomQuestion07> values, final Context context)
                throws IOException, InterruptedException {

            double somaTotal = 0;
            int totalCount = 0;

            for (CustomQuestion07 val : values) {
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
