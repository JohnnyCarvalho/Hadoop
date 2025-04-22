package basic;

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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        //registrar as classes
        //definir os tipos de entrada e saída
        //cadastro dos arquivos de entrada e saída
        //criar o exit do programa

        // 1 - registrar classes
        j.setJarByClass(WordCount.class);
        j.setMapperClass(MapForWordCount.class);
        j.setReducerClass(ReduceForWordCount.class);

        //2 - definir os tipos de entrada e saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //3 - cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //4 - criar o exit do programa
        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }

    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // map recebe os dados do bloco
            // más na verdade ela recebe a linha

            // line 1:1 In the beginning God created the heaven and the earth.

            String line = value.toString();

            String[] words = line.split(" ");

            // criar uma tupla de saída, onde essa tupla será uma lista de chave e valor
            // chave: palavra
            // valor: 1

            for (String word : words) {
                Text outputKey = new Text(word.toUpperCase().trim());
                IntWritable outputValue = new IntWritable(1);
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            //chave = List<values> Ex --> <God, <1, 1, 1, 1, 1>>

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            con.write(key, new IntWritable(sum));

        }
    }

}
