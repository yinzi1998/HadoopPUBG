import java.io.IOException;
import java.util.StringTokenizer;
import java.math.*;

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
			float x1 = 0,y1 = 0;
			float x2 = 0,y2 = 0;
			float dis;

			for(String s:line1){
				String[] str = s.split(",");
				if(str.length>=1){
					if(str[3].length()!=0 && str[4].length() != 0&&str[10].length()!=0 && str[11].length()!=0){
						x1 = Float.parseFloat(str[3]);
						y1 = Float.parseFloat(str[4]);
						x2 = Float.parseFloat(str[10]);
						y2 = Float.parseFloat(str[11]);

						dis = (float)Math.sqrt((x1-x2)*(x1-x2)+(y1-y2)*(y1-y2)); 

						word.set("kill_distance");
						context.write(word,new FloatWritable(dis));
					}
					
				}
			}			
		}
	}

	public static class WeaponReduce extends Reducer<Text, FloatWritable, Text, Text> {
		private Text result = new Text();
		private Text num = new Text();
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {

			for (FloatWritable val : values) {

				result.set(String.valueOf(val.get()));
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
