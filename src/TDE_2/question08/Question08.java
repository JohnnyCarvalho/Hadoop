package TDE_2.question08;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Question08 {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        BasicConfigurator.configure();
        Configuration config = new Configuration();

        final Path input = new Path("in/data_tde_2.csv");
        final Path output = new Path("output/question08");
        final Path intermediateMin = new Path("output/intermediate/question08_min");
        final Path intermediateMax = new Path("output/intermediate/question08_max");

        final Job job1 = Job.getInstance(config, "Transação mínima por ano e país");
        job1.setJarByClass(Question08.class);
        job1.setMapperClass(MapTransactionMin.class);
        job1.setReducerClass(ReduceTransactionMin.class);
        job1.setMapOutputKeyClass(CountryYearWritable.class);
        job1.setMapOutputValueClass(DoubleWritable.class);
        job1.setOutputKeyClass(CountryYearWritable.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, intermediateMin);

        final Job job2 = Job.getInstance(config, "Transação máxima por ano e país");
        job2.setJarByClass(Question08.class);
        job2.setMapperClass(MapTransactionMax.class);
        job2.setReducerClass(ReduceTransactionMax.class);
        job2.setMapOutputKeyClass(CountryYearWritable.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setOutputKeyClass(CountryYearWritable.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, input);
        FileOutputFormat.setOutputPath(job2, intermediateMax);

        final Job job3 = Job.getInstance(config, "Transações mínimas e máximas consolidadas");
        job3.setJarByClass(Question08.class);
        job3.setMapperClass(MapTransactionOutput.class);
        job3.setReducerClass(ReduceTransactionOutput.class);
        job3.setMapOutputKeyClass(CountryYearWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(CustomQuestion08.class);
        job3.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job3, intermediateMin);
        FileInputFormat.addInputPath(job3, intermediateMax);
        FileOutputFormat.setOutputPath(job3, output);

        if (job1.waitForCompletion(true)) {
            if (job2.waitForCompletion(true)) {
                job3.waitForCompletion(true);
            }
        }
    }

    public static class MapTransactionMin extends Mapper<LongWritable, Text, CountryYearWritable, DoubleWritable> {
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("country_or_area")) return;
            final String[] parts = value.toString().split(";");
            final String country = parts[0].trim();
            final String year = parts[1].trim();
            final String tradeUsd = parts[5].trim().replace(".", "").replace(",", ".");
            try {
                final double amount = Double.parseDouble(tradeUsd);
                context.write(new CountryYearWritable(country, year), new DoubleWritable(amount));
            } catch (NumberFormatException ignored) {}
        }
    }

    public static class ReduceTransactionMin extends Reducer<CountryYearWritable, DoubleWritable, CountryYearWritable, DoubleWritable> {
        public void reduce(final CountryYearWritable key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            for (DoubleWritable val : values) {
                min = Math.min(min, val.get());
            }
            context.write(key, new DoubleWritable(min));
        }
    }

    public static class MapTransactionMax extends Mapper<LongWritable, Text, CountryYearWritable, DoubleWritable> {
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            if (value.toString().startsWith("country_or_area")) return;
            final String[] parts = value.toString().split(";");
            final String country = parts[0].trim();
            final String year = parts[1].trim();
            final String tradeUsd = parts[5].trim().replace(".", "").replace(",", ".");
            try {
                double amount = Double.parseDouble(tradeUsd);
                context.write(new CountryYearWritable(country, year), new DoubleWritable(amount));
            } catch (NumberFormatException ignored) {}
        }
    }

    public static class ReduceTransactionMax extends Reducer<CountryYearWritable, DoubleWritable, CountryYearWritable, DoubleWritable> {
        public void reduce(final CountryYearWritable key, final Iterable<DoubleWritable> values, final Context context) throws IOException, InterruptedException {
            double max = Double.MIN_VALUE;
            for (DoubleWritable val : values) {
                max = Math.max(max, val.get());
            }
            context.write(key, new DoubleWritable(max));
        }
    }

    public static class MapTransactionOutput extends Mapper<LongWritable, Text, CountryYearWritable, Text> {
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
            final String[] parts = value.toString().split("\t");
            if (parts.length == 3) {
                CountryYearWritable k = new CountryYearWritable(parts[0], parts[1]);
                context.write(k, new Text(parts[2]));
            }
        }
    }

    public static class ReduceTransactionOutput extends Reducer<CountryYearWritable, Text, CountryYearWritable, CustomQuestion08> {
        public void reduce(final CountryYearWritable key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            for (Text val : values) {
                try {
                    final double v = Double.parseDouble(val.toString());
                    if (v < min) min = v;
                    if (v > max) max = v;
                } catch (NumberFormatException ignored) {}
            }
            final CustomQuestion08 result = new CustomQuestion08(key.getCountry(), key.getYear(), min, max);
            context.write(key, result);
        }
    }

}
