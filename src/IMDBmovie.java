import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import java.io.IOException;

public class IMDBmovie extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(IMDBmovie.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new IMDBmovie(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "IMDBmovie");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        //SequenceFileOutputFormat.setOutputPath(job, new Path(args[2]));
        SequenceFileAsBinaryOutputFormat.setOutputPath(job, new Path(args[2]));
        //FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(IMDBMapper.class);
        job.setCombinerClass(IMDBCombiner.class);
        job.setReducerClass(IMDBReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    //public static class AvgSizeStationMapper extends
    // Mapper<LongWritable, Text, Text, IntWritable>
    public static class IMDBMapper  extends Mapper<LongWritable, Text, Text,  IntWritable> {
        //klucz wejściowy (pozycja rekordu w pliku) <> wartość wejściowa ( wiersz tekstu)
        // <> klucz wyjściowy (tconst) <> wartość wyjściowa funkcji mapującej (category)
        private Text tconst = new Text();
        private  IntWritable number = new  IntWritable();

        public void map(LongWritable key, Text lineText, Context context)
                throws IOException, InterruptedException {
            try {
                if (key.get() == 0)
                    return;
                else {
                    String line = lineText.toString();
                    // tconst (string) * ordering (int)  * nconst (string)  * category (string)
                    // * job (string) * characters (string)
                    int i = 0;
                    for (String token : line.split("\t")) {
                        if (i == 0) {
                            tconst.set(token);
                        }
                        if (i == 3) {
                            if (token.equals("actor") || token.equals("actress")) {
                                number.set(1);
                            } else {
                                number.set(0);
                            }
                            context.write(tconst, number);
                        }
                        i++;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class IMDBCombiner extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        int count;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            result.set(count);
            context.write(key, result);
        }
    }

    public static class IMDBReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        int count;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            if(count != 0) {
                result.set(count);
                context.write(key, result);
            }
        }
    }
}