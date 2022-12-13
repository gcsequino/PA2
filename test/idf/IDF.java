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
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.io.IOException;

public class IDF extends Configured implements Tool{

    public static class IDFMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
            // line = [term, doc_id, tf]
            String line = value.toString();
            String[] line_arr = line.split("\t");
            context.write(new Text(line_arr[0]), new Text(line_arr[1]));
        }
    }

    public static class IDFReducer extends Reducer<Text,Text,Text,Text>{

        double doc_count;

        @Override
        protected void setup(Reducer<Text,Text,Text,Text>.Context context)
        throws IOException, InterruptedException {
            if(context.getCacheFiles() != null && context.getCacheFiles().length > 0){
                File f = new File("./doc_count");
                Scanner s = new Scanner(f);
                String line = "";
                if(s.hasNextLine()) line = s.nextLine();
                line = line.replaceAll("[^A-Za-z0-9]", "");
                doc_count = Double.parseDouble(line);
            }
            super.setup(context);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
            double term_docs = 0;
            double total_docs = doc_count;
            //total_docs = 101396;
            Set<String> my_set = new TreeSet<>();
            for(Text value : values){
                String val = value.toString();
                my_set.add(val);
            }
            term_docs = my_set.size();
            double idf = Math.log10(total_docs / term_docs);
            context.write(key, new Text(Double.toString(idf)));
        }
    }

    public static void main(String[] args) throws Exception { 
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN 
		int res = ToolRunner.run(new Configuration(), new IDF(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "unigrams date"); 

        job.setJarByClass(IDF.class); 
        job.setMapperClass(IDF.IDFMapper.class); 
        job.setReducerClass(IDF.IDFReducer.class);

        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

        job.addCacheFile(new URI("hdfs:///PA2/out_doc_count/out.txt#doc_count"));

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}