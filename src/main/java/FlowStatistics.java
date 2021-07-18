
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;

public class FlowStatistics {

	public static class FlowMapper extends Mapper<Object, Text, Text, Text>{

		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			if(strs.length == 11) {
				Text phone = new Text(strs[1]);
				Text flow = new Text(strs[8]+"\t"+strs[9]);
				context.write(phone, flow);
			}

		}
	}
	
	public static class FlowReducer extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int upFlow = 0;
			int downFlow = 0;
			
			for (Text value : values) {
				String[] strs = value.toString().split("\t");
				upFlow += Integer.parseInt(strs[0].toString());
				downFlow += Integer.parseInt(strs[1].toString());
			}
			int sumFlow = upFlow+downFlow;
			
			context.write(key,new Text(upFlow+"\t"+downFlow+"\t"+sumFlow));
		}
	}
	
	// 第二种写法
	public static class FlowWritableMapper extends Mapper<Object, Text, Text, FlowBean> {
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");

			if(strs.length == 11) {
				Text phone = new Text(strs[1]);
				FlowBean flow = new FlowBean(Integer.parseInt(strs[8]),Integer.parseInt(strs[9]));
				context.write(phone, flow);
			}

		}
	}
	public static class FlowWritableReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		public void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
			int upFlow = 0;
			int downFlow = 0;
			
			for (FlowBean value : values) {
				upFlow += value.getUpFlow();
				downFlow += value.getDownFlow();
			}
//			System.out.println(key.toString()+":"+upFlow+","+downFlow);
			context.write(key,new FlowBean(upFlow,downFlow));
		}
	}
	
	public static class FlowBean implements Writable{
		private int upFlow;
		private int downFlow;
		private int sumFlow;
		
		public FlowBean() {}

		public FlowBean(int upFlow, int downFlow) {
			this.upFlow = upFlow;
			this.downFlow = downFlow;
			this.sumFlow = upFlow+downFlow;
		}
		
		public int getDownFlow() {
			return downFlow;
		}

		public void setDownFlow(int downFlow) {
			this.downFlow = downFlow;
		}

		public int getUpFlow() {
			return upFlow;
		}

		public void setUpFlow(int upFlow) {
			this.upFlow = upFlow;
		}

		public int getSumFlow() {
			return sumFlow;
		}

		public void setSumFlow(int sumFlow) {
			this.sumFlow = sumFlow;
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(upFlow);
			out.writeInt(downFlow);
			out.writeInt(sumFlow);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			upFlow = in.readInt();
			downFlow = in.readInt();
			sumFlow = in.readInt();
		}

		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return upFlow+"\t"+downFlow+"\t"+sumFlow;
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "flow statistics");

		job.setJarByClass(FlowStatistics.class);
		//第一种写法
//		job.setMapperClass(FlowMapper.class);
//		job.setCombinerClass(FlowReducer.class);
//		job.setReducerClass(FlowReducer.class);
//
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(Text.class);

		//第二种写法
		job.setMapperClass(FlowWritableMapper.class);
		job.setCombinerClass(FlowWritableReducer.class);
		job.setReducerClass(FlowWritableReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));


		Path outPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPath);
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
}
