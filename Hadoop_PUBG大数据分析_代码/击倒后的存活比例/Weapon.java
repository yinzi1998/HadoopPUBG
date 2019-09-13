import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

	public static class WeaponMap extends Mapper<LongWritable, Text, Text, Text> {

		private final Text one = new Text();
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] line1=line.split("\n");
			for(String s:line1){
				String[] str=s.split(",");
				if(str.length>=1&&!(str[3].equals("match_id"))&&!(str[7].equals("player_dbno"))&&!(str[11].equals("player_kills"))){
					word.set(str[3]);
					one.set(str[7]+","+str[11]);
					context.write(word,one);
				}
			}
		}
	}

	public static class WeaponReduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float sum_dbno = 0,sum_kills = 0;
			float res=0,count=0,dbno=0,kills=0;
			String temp="";
			for(Text s:values){
				temp=s.toString();
				String[] str=temp.split(",");
				dbno=Float.parseFloat(str[0]);
				kills=Float.parseFloat(str[1]);
				count++;
				sum_dbno+=dbno;
				sum_kills+=kills;
				
			}
			res=(sum_dbno+sum_kills)/count;
			result.set(String.valueOf(res));
			context.write(key, result);
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Weapon.class);
		job.setJobName("weapon");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(WeaponMap.class);
		job.setReducerClass(WeaponReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
