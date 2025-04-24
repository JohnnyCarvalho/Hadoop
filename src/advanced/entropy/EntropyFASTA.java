package advanced.entropy;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class EntropyFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/JY157487.1.fasta");

        Path intermediate = new Path("./output/intermediate.tmp");

        // arquivo de saida
        Path output = new Path("output/fastaEntropy");

        Job j1 = new Job(c, "etapa A");
        Job j2 = new Job(c, "etapa B");

        // Registro das Classes
        j1.setJarByClass(EntropyFASTA.class);
        j1.setMapperClass(MapEtapaA.class);
        j1.setReducerClass(ReduceEtapaA.class);

        j2.setJarByClass(EntropyFASTA.class);
        j2.setMapperClass(MapEtapaB.class);
        j2.setReducerClass(ReduceEtapaB.class);

        // Definição dos tipos de entrada e saída
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(LongWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(LongWritable.class);

        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(BaseQtdWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);

        // Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        // Criação do job e seu nome
        j1.waitForCompletion(true);

        j2.waitForCompletion(true);

    }

    public static class MapEtapaA extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Ignorar linhas de cabaçalho
            if (line.startsWith(">")) {
                return;
            }

            // Dividir a linha em caracteres
            String[] characters = line.split("");

            for (String character : characters) {
                con.write(new Text(character), new LongWritable(1));
                con.write(new Text("total"), new LongWritable(1));
            }
        }
    }

    public static class ReduceEtapaA extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            long sum = 0;

            for (LongWritable value : values) {
                sum += value.get();
            }

            con.write(key, new LongWritable(sum));

        }
    }


    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();

            String[] characters = line.split("\t");

            con.write(new Text("total"), new BaseQtdWritable(characters[0], Long.parseLong(characters[1])));

        }
    }

    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
                throws IOException, InterruptedException {

            long total = 0;
            for (BaseQtdWritable value : values) {
                if (value.getBase().equals("total")) {
                    total = value.getQtd();
                    break;
                }
            }

            for (BaseQtdWritable value : values) {
                if (!value.getBase().equals("total")) {
                    double probability = (double) value.getQtd() / total;
                    double entropy = -probability * Math.log10(probability) / Math.log10(2);
                    con.write(new Text(value.getBase()), new DoubleWritable(entropy));
                }
            }
        }
    }
}
