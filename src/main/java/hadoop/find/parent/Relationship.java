package hadoop.find.parent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        import java.io.IOException;
        import java.util.ArrayList;
        import java.util.StringTokenizer;

public class Relationship {

    public static class RsMapper extends Mapper<Object, Text, Text, Text> {

        private static int linenum = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (linenum == 0) {
                ++linenum;
            } else {
                System.out.println("mapper:"+value.toString());
                StringTokenizer tokenizer = new StringTokenizer(line, "\n");
                while (tokenizer.hasMoreElements()) {
                    StringTokenizer lineTokenizer = new StringTokenizer(tokenizer.nextToken());
                    String son = lineTokenizer.nextToken();
                    System.out.println("mapper inner: son"+son);
                    String parent = lineTokenizer.nextToken();
                    System.out.println("mapper inner: son"+parent);
                    context.write(new Text(parent), new Text(
                            "-" + son));
                    context.write(new Text(son), new Text
                            ("+" + parent));
                }
            }
        }
    }

    public static class RsReducer extends Reducer<Text, Text, Text, Text> {
        private static int linenum = 0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            if (linenum == 0) {
                context.write(new Text("grandson"), new Text("grandparent"));
                ++linenum;
            }
            ArrayList<Text> grandChild = new ArrayList<Text>();
            ArrayList<Text> grandParent = new ArrayList<Text>();

            for (Text val : values) {
                String s = val.toString();

                if (s.startsWith("-")) {
                    grandChild.add(new Text(s.substring(1)));
                } else {
                    grandParent.add(new Text(s.substring(1)));
                }
            }

            for (Text text1 : grandChild) {
                for (Text text2 : grandParent) {
                    context.write(text1, text2);
                }
            }


        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration cong = new Configuration();

        String[] otherArgs = new String[]{"/home/latham/Desktop/hadoop/test/relationship",
                "/home/latham/Desktop/hadoop/test/output"};
        if (otherArgs.length != 2) {
            System.out.println("参数错误");
            System.exit(2);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Relationship.class);
        job.setMapperClass(RsMapper.class);
        job.setReducerClass(RsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}