package hadoop.file.merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;

public class FileMerge {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text text = new Text();

        public void map(Object key, Text value, Context content) throws IOException, InterruptedException {
            text = value;
            System.out.println("map:"+key.toString()+"\t"+text.toString());
            content.write(text, new Text("1"));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//            System.out.printf("reduce:"+key.toString()+"\t"+text.toString());
            context.write(key, new Text("2"));
        }
    }


    public static void main(String[] args) throws Exception {

        // delete output directory
//        FileUtil.deleteDir("output");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        String[] otherArgs = new String[]{"/home/latham/Desktop/hadoop/test/*",
                "/home/latham/Desktop/hadoop/test/output"};
        if (otherArgs.length != 2) {
            System.err.println("Usage:Merge and duplicate removal <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance();
        job.setJarByClass(FileMerge.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
