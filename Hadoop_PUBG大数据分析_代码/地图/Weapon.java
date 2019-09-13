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
			float x = 0,y = 0;
			int final_x = 0,final_y = 0,position= 0;
			for(String s:line1){
				String[] str = s.split(",");
				if(str.length>=1&&!(str[10].equals("victim_position_x"))&&!(str[11].equals("victim_position_y"))){
					x = Float.parseFloat(str[10]);
					y = Float.parseFloat(str[11]);
					final_x = (int)(x/8000);
					final_y = (int)(y/8000);
					position = final_y*100+final_x;
					if(!(str[5].equals("\\s+"))){ 
						word.set(str[5]+","+position);
						context.write(word,one);
					}
				}
			}			
		}
	}

	public static class WeaponReduce extends Reducer<Text, IntWritable, Text, Text> {
		private Text result = new Text();
		private Text num = new Text();
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			if(key.toString().split(",")[0].equals("MIRAMAR")){
			for (IntWritable val : values) {
				sum += val.get();
					
			}
			result.set(String.valueOf(sum));
			num.set(key.toString().split(",")[1]);
			context.write(num, result);	
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
