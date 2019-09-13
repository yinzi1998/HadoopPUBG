import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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

	public static class WeaponMap extends Mapper<LongWritable, Text, Text, FloatWritable> {

		private final FloatWritable one = new FloatWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] line1 = line.split("\n");
			float max = 0;
			float min = 1000000;
			float temp = 0;
			for(String s:line1){
				String[] str = s.split("\t");
				//System.out.println(str.length+"aaaaaaaaaaaaaaaaaaaaaaaa");
				if(str.length>=1){
					temp = Float.parseFloat(str[1]);
					if(temp>max){
						max = temp;						
					}
					if(temp<min){
						min = temp;		
					}
				}
			}
			word.set("max");
			context.write(word,new FloatWritable(max));
			word.set("min");
			context.write(word,new FloatWritable(min));
		}
	}

	public static class WeaponReduce extends Reducer<Text, FloatWritable, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			float max = 0;
			float min = 1000000; 
			float temp = 0;
			if(key.toString().equals("max")){
				for (FloatWritable val : values) {
					temp= val.get();
					if(temp>max){
						max = temp;				
					}	
				}
				result.set(String.valueOf(max));
				context.write(key, result);
			}
		
			if(key.toString().equals("min")){
				for (FloatWritable val : values) {
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
		job.setOutputValueClass(FloatWritable.class);

		job.setMapperClass(WeaponMap.class);
		job.setReducerClass(WeaponReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}

