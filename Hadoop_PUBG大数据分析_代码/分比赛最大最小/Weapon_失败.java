import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Weapon {

	public static class WeaponMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] line1 = line.split("\n");
			int max = 0;
			int min = 1000000;
			int temp = 0;
			String temp1="";

			if(line1[0].split(",").length>=1){
				temp1 = line1[0].split(",")[2];
			}
						
			for(String s:line1){
				String[] str = s.split(",");
				if(str.length>=1){
					if(str[2].equals(temp1)){
						temp = Integer.parseInt(str[12]);
						if(temp>max){
							max = temp;						
						}
						if(temp<min){
							min = temp;		
						}
					}
					else{
						word.set(temp1.substring(temp1.length()-6,temp1.length())+"max");
						context.write(word,new IntWritable(max));
						word.set(temp1.substring(temp1.length()-6,temp1.length())+"min");
						context.write(word,new IntWritable(min));
						temp1=str[2];
						max=0;
						min=1000000;
						temp = Integer.parseInt(str[12]);
						if(temp>max){
							max = temp;						
						}
						if(temp<min){
							min = temp;		
						}
					}
					
				}
			}
			
		}
	}

	public static class WeaponReduce extends Reducer<Text, IntWritable, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int max = 0;
			int min = 1000000; 
			int temp = 0;

			if(key.toString().substring(key.toString().length()-3, key.toString().length()).equals("max")){
				for (IntWritable val : values) {
					temp= val.get();
					if(temp>max){
						max = temp;				
					}	
				}
				result.set(String.valueOf(max));
				context.write(key, result);
			}
		
			if(key.toString().substring(key.toString().length()-3, key.toString().length()).equals("min")){
				for (IntWritable val : values) {
					temp= val.get();
					if(temp<min){
						min = temp;				
					}	
				}
				result.set(String.valueOf(min));
				context.write(key, result);
			}
				
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Weapon.class);
		job.setJobName("weapon");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WeaponMap.class);
		job.setReducerClass(WeaponReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
