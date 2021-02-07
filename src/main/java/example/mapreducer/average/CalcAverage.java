package example.mapreducer.average;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class CalcAverage {
    public static class ClassifyNumbers extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, IntWritable, IntWritable> {

        public void map(LongWritable key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
            int numOfNodes = 3;
            int idx = new Random().nextInt(numOfNodes);

            IntWritable node = new IntWritable(idx);
            IntWritable number = new IntWritable(Integer.parseInt(value.toString()));
            output.collect(node, number);
        }
    }

    public static class CalSumAndAvg extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            int count = 0;
            while (values.hasNext()) {
                sum += values.next().get();
                count = count + 1;
            }
            output.collect(new IntWritable(count), new IntWritable(sum));
        }
    }

    public static class MapSumAndCounts extends MapReduceBase implements org.apache.hadoop.mapred.Mapper<LongWritable, Text, Text, Text> {

        private final Text sumAndCount = new Text("sumAndCount");

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            output.collect(sumAndCount, value);
        }
    }

    public static class AverageCalcReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double sum = 0;
            int count = 0;
            while (values.hasNext()) {
                String[]  list = values.next().toString().split("\\s+");
                sum += Integer.parseInt(list[1]);
                count += Integer.parseInt(list[0]);
            }

            String avg = String.valueOf(sum / count);
            output.collect(new Text("average : "), new Text(avg));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf1 = new JobConf(CalcAverage.class);
        conf1.setJobName("classify-numbers");
        conf1.setOutputKeyClass(IntWritable.class);
        conf1.setOutputValueClass(IntWritable.class);
        conf1.setMapperClass(ClassifyNumbers.class);
        conf1.setReducerClass(CalSumAndAvg.class);
        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));
        JobClient.runJob(conf1).waitForCompletion();

        JobConf conf2 = new JobConf(CalcAverage.class);
        conf2.setJobName("average");
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        conf2.setMapperClass(MapSumAndCounts.class);
        conf2.setReducerClass(AverageCalcReducer.class);
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf2, new Path(args[1]));
        FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
        JobClient.runJob(conf2);
    }
}
