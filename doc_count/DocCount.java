import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.io.IOException;

public class DocCount extends Configured implements Tool{

    public static class DocCountMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
            String line = value.toString();
            if(line.contains("<====>")) context.write(new Text("one"), new Text("one"));
        }
    }

    public static class DocCountReducer extends Reducer<Text,Text,Text,IntWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            int count = 0;
            for(Text val : values) count++;
            context.write(new Text(""), new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception { 
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN 
		int res = ToolRunner.run(new Configuration(), new DocCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "unigrams date"); 

        job.setJarByClass(DocCount.class); 
        job.setMapperClass(DocCount.DocCountMapper.class); 
        job.setReducerClass(DocCount.DocCountReducer.class);

        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}