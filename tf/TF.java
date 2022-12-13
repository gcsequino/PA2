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
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.io.IOException;

public class TF extends Configured implements Tool{

    public static class TFMapper extends Mapper<LongWritable, Text, Text, Text>{

        static String title;
        static String id;

        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{

            String curr_line = value.toString();

            if(curr_line.contains("<====>")){
                String[] curr_line_arr = curr_line.split("<====>");
                if(curr_line_arr.length < 3) return;
                title = curr_line_arr[0];
                id = curr_line_arr[1];
                curr_line = curr_line_arr[2];
            } else { return; }
            
            // format content
            curr_line = curr_line.toLowerCase()
                                 .replaceAll("[^A-Za-z\\s]", "")
                                 .replaceAll("\\s{2,}", " ")
                                 .trim();
            String[] tokens = curr_line.split(" ");

            // populate map
            Map<String, Integer> my_map = new TreeMap<String, Integer>();
            for (String token : tokens) {
                if(my_map.containsKey(token)){
                    my_map.put(token, my_map.get(token)+1);
                } else {
                    my_map.put(token, 1);
                }
            }

            // find max freq
            double max_freq = 0;
            for(Entry<String, Integer> entry : my_map.entrySet()) {
                if(max_freq < entry.getValue())
                    max_freq = entry.getValue();
            }

            // calc TF for each term
            for(Entry<String, Integer> entry : my_map.entrySet()) {
                double tf = 0.5 + (0.5 * (entry.getValue() / max_freq));
                context.write(new Text(entry.getKey().toString() + "\t" + id), new Text(tf + ""));
            }
        }
    }

    public static void main(String[] args) throws Exception { 
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN 
		int res = ToolRunner.run(new Configuration(), new TF(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "tf"); 

        job.setJarByClass(TF.class); 
        job.setMapperClass(TF.TFMapper.class); 
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}