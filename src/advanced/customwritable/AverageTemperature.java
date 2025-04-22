package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // 1 - registrar classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //2 - definir os tipos de entrada e saída
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FireAvgTempWritable.class);
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);

        //3 - cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //4 - criar o exit do programa
        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            // 7,5,mar,fri,86.2,26.2,94.3,5.1,8.2,51,6.7,0,0 --> coluna 8

            String line = value.toString();
            String[] words = line.split(",");

            float temp = Float.parseFloat(words[7]);

            con.write(new Text("common"), new FireAvgTempWritable(1, temp));

        }
    }

//    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
//        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
//                throws IOException, InterruptedException {
//        }
//    }


    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            int sumN = 0;
            float sumTemperature = 0;

            for (FireAvgTempWritable val : values) {
                sumN += val.getN();
                sumTemperature += val.getTemperature();
            }

            float average = sumTemperature / sumN;

            con.write(new Text("average"), new FloatWritable(average));
        }
    }

}
