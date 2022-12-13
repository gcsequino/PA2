import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.*;
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
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.io.IOException;

public class TFIDF extends Configured implements Tool{

    public static class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text>{

        static Map<String, Double> map = new TreeMap<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
            if(context.getCacheFiles() != null && context.getCacheFiles().length > 0){
                File f = new File("./idf");
                Scanner s = new Scanner(f);
                String line = "";
                while(s.hasNextLine()){
                    line = s.nextLine();
                    String[] arr = line.split("\t");
                    String key = arr[0];
                    Double val = Double.parseDouble(arr[1]);
                    map.put(key, val);
                }
            }
            //context.write(new Text(map.toString()), new Text(""));
        }

        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
            String line = value.toString();
            String[] arr = line.split("\t");
            String term = arr[0];
            String doc_id = arr[1];
            double tf = 0;
            //tf = Double.parseDouble(arr[2]);
            try{
                tf = Double.parseDouble(arr[2]);
            } catch(NumberFormatException e){
                // oop
                return;
            }
            double idf = map.get(term);
            double tfidf = tf * idf;
            context.write(new Text(term + "\t" + doc_id), new Text(Double.toString(tfidf)));
        }
    }

    public static void main(String[] args) throws Exception { 
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN 
		int res = ToolRunner.run(new Configuration(), new TFIDF(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "unigrams date");

        job.setJarByClass(TFIDF.class); 
        job.setMapperClass(TFIDF.TFIDFMapper.class); 
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

        job.addCacheFile(new URI("hdfs:///PA2/out_idf/out.txt#idf"));

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}