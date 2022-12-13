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
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.io.IOException;

public class Sum extends Configured implements Tool{

    public static class Sentence implements Comparable<Sentence>{

        public static class Pair implements Comparable<Pair>{
            String x;
            double y;

            public Pair(String x, double y){
                this.x = x;
                this.y = y;
            }

            @Override
            public int compareTo(Sum.Sentence.Pair o) {
                double diff = this.y - o.y;
                if(diff == 0) return 0;
                if(diff < 0) return -1;
                else return 1;
            }
        }

        double score;
        int index;
        String[] terms;

        public Sentence(String data, int index, Map<String, Double> map) {
            this.index = index;
            String[] arr = data.split("\\s+");
            PriorityQueue<Pair> heap = new PriorityQueue<>();

            for(String term : arr){
                Pair p = new Pair(term, map.getOrDefault(term, 0.0));
                heap.add(p);
                if(heap.size() > 5) heap.poll();
            }
            terms = new String[5];
            int i = 0;
            for(Pair p : heap){
                terms[i++] = p.x;
                score += p.y;
            }
        }

        @Override
        public int compareTo(Sum.Sentence o) {
            double diff = this.score - o.score;
            if(diff < 0) return -1;
            if(diff == 0) return 0;
            else return 1;
        }
    }

    public static class SumMapper extends Mapper<LongWritable, Text, Text, Text>{

        static Map<String, Double> map = new TreeMap<>();

        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
        throws IOException, InterruptedException {
            
            if(context.getCacheFiles() != null && context.getCacheFiles().length > 0){
                File f = new File("./tf_idf");
                Scanner s = new Scanner(f);
                String line = "";
                while(s.hasNextLine()){
                    line = s.nextLine();
                    String[] arr = line.split("\t");
                    String term = arr[0];
                    String doc_id = arr[1];
                    String key = term + doc_id;
                    Double val = Double.parseDouble(arr[2]);
                    map.put(key, val);
                }
            }
            //context.write(new Text(map.toString()), new Text(""));
            
        }

        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
            
            String line = value.toString();
            if(!line.contains("<====>")) return;
            String[] arr = line.split("<====>");
            if(arr.length != 3) return;
            String[] doc = arr[2].split("(?<=[a-z])\\.\\s+");

            PriorityQueue<Sentence> heap = new PriorityQueue<>();
            int i = 0;
            for(String sen : doc){
                //context.write(new Text(sen), new Text(""));
                Sentence sentence = new Sentence(sen, i++, map);
                heap.add(sentence);
                if(heap.size() > 5) heap.poll();
            }

            Comparator<Sentence> byIndex =
                (Sentence s1, Sentence s2) -> Integer.compare(s1.index, s2.index);

            String res = heap.stream().sorted(byIndex)
                         .map(sentence -> Arrays.toString(sentence.terms))
                         .collect(Collectors.joining(" "));
            context.write(new Text(arr[1]), new Text(res));
            
        }
    }

    public static void main(String[] args) throws Exception { 
		//ToolRunner allows for command line configuration parameters - suitable for shifting between local job and yarn
		// example command: hadoop jar <path_to_jar.jar> <main_class> -D param=value <input_path> <output_path>
		//We use -D mapreduce.framework.name=<value> where <value>=local means the job is run locally and <value>=yarn means using YARN 
		int res = ToolRunner.run(new Configuration(), new Sum(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "unigrams date");

        job.setJarByClass(Sum.class); 
        job.setMapperClass(Sum.SumMapper.class); 
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

        job.addCacheFile(new URI("hdfs:///PA2/out_tf_idf/out.txt#tf_idf"));

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }
}