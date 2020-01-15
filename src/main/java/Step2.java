
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Step2 {
    static final String END_OF_KEYS = "\uFFFF";

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private Text _key = new Text();
        private Text val = new Text();
        private String w1, w2, w3, space = " ";

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("MAPPER TEXT IS: ");
            System.out.println(value.toString());
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
            while (itr.hasMoreTokens()) {

                String[] parts = itr.nextToken().split("\t");
                String[] key_parts = parts[0].split(" ");
                val.set(parts[1]);
                w1 = key_parts[0];
                w2 = key_parts[1];
                w3 = key_parts[2];

                // (<w1>, #)
                _key.set(w1);
                context.write(_key, val);
                // (<w2>, #)
                _key.set(w2);
                context.write(_key, val);
                // (<w3>, #)
                _key.set(w3);
                context.write(_key, val);
                // (<w1 w2 1 1>, #)
                _key.set(w1 + space + w2 + space + "1 1");
                context.write(_key, val);
                // (<w2 w3 2 2>, #)
                _key.set(w2 + space + w3 + space + "2 2");
                context.write(_key, val);
                // (<w1 w2 w3>, #)
                _key.set(w1 + space + w2 + space + w3);
                context.write(_key, val);

            }
            // (<end end>, #)
            _key.set(END_OF_KEYS + space + END_OF_KEYS);
            context.write(_key, val);
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        private int total = 0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("REDUCER KEY IS: ");
            System.out.println(key.toString());
            int sum = 0;
            String[] parts = key.toString().split(" ");

            for (Text value : values) {
//                if (!value.toString().contains(" ")) {
                sum += Integer.parseInt(value.toString());
//                } else {
//                    context.write(key, value);
//                }
            }
            if (key.toString().contains(END_OF_KEYS)) { //TODO if not need the end of key- put before + continue;
                context.write(new Text(". . . ."), new Text(String.valueOf(total)));  //total num of words
                return;
            }
            context.write(key, new Text(String.valueOf(sum)));

            if (parts.length == 1) {
                System.out.println("totaling" + total);
                total = total + sum;
            }

        }

    }

    public static class PartitionerClass extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numPartitions) {

            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, " Step 2");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}