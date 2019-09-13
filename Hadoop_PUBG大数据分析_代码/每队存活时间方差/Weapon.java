import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
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

	public static class WeaponMap extends Mapper<LongWritable, Text, Text, FloatWritable> {

		private Text word = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] line1 = line.split("\n");
			float x = 0;
			for(String s:line1){
				String[] str = s.split(",");
				if(str.length>=1&&!(str[2].equals("match_id"))){
					if(Integer.parseInt(str[4])>1){
						x = Float.parseFloat(str[12]);
						word.set(str[2]+","+str[13]);
						context.write(word,new FloatWritable(x));
					}
				}
			}			
		}
	}

	public static class WeaponReduce extends Reducer<Text, FloatWritable, Text, Text> {
		private Text result = new Text();
		private Text num = new Text();
		private ArrayList<Float> list;
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			float sum = 0;
			float std = 0;
			int count = 0;
			list = new ArrayList<>();
			for(FloatWritable val: values){
				sum += val.get();
				list.add(val.get());
				count++;
			}
			for(float f: list){
				std += ((float)(sum/count)-f)*((float)(sum/count)-f);
			}
			std = (float)(std/count);
			result.set(Float.toString(std));
			if(count>1){
				context.write(key,result);
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
