package TDE_2.question06;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Question06 {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration config = new Configuration();

        Path input = new Path("in/data_tde_2.csv");
        Path output = new Path("output/question06");

        Job job = Job.getInstance(config, "Transação mais cara e mais barata no Brasil em 2016");

        job.setJarByClass(Question06.class);
        job.setMapperClass(MapTransactionExtremes.class);
        job.setReducerClass(ReduceTransactionExtremes.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }

    public static class MapTransactionExtremes extends Mapper<LongWritable, Text, Text, Text> {
        private final static Text outKey = new Text("Transacoes_Brasil_2016");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("country_or_area")) {
                return;
            }
            String[] fields = value.toString().split(";");
            if (fields.length > 5) {
                String country = fields[0].trim();
                String yearStr = fields[1].trim();
                String valueStr = fields[5].trim();

                if (country.equalsIgnoreCase("Brazil") && yearStr.equals("2016")) {
                    try {
                        valueStr = valueStr.replace(".", "").replace(",", ".");
                        double valor = Double.parseDouble(valueStr);
                        context.write(outKey, new Text(String.format("%.4f", valor)));
                    } catch (NumberFormatException e) {
                        System.out.println("Erro ao converter valor: " + e.getMessage());
                    }
                }
            }
        }
    }

    public static class ReduceTransactionExtremes extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;

            for (Text val : values) {
                try {
                    double value = Double.parseDouble(val.toString().replace(",", "."));
                    if (value < min) min = value;
                    if (value > max) max = value;
                } catch (NumberFormatException e) {
                    // ignora valores inválidos
                }
            }

            context.write(new Text("Transaction min"), new Text(String.valueOf(min)));
            context.write(new Text("Transaction max"), new Text(String.valueOf(max)));
        }
    }
}
