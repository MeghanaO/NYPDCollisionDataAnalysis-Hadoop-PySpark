package com.hadoop.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

// This main class holds the Driver, Mapper class, Partitioner class and Reducer class 
public class WithPartitioner {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf= new Configuration();
		// Assigning a name for the job that has the whole map and reduce operation
		Job job = new Job(conf,"WordCount");
		// Specifying the main class name that has the driver
		job.setJarByClass(WithPartitioner.class);
		// Specifying the mapper class
		job.setMapperClass(MapperForWordCount.class);
		// Specifying the partitioner class
		job.setPartitionerClass(PartitionerForWordCount.class);
		// Specifying the reducer class
		job.setReducerClass(ReducerForWordCount.class);
		// Specifying the number of reducer tasks
		job.setNumReduceTasks(8);
		// Specifying the object type for reducer output Key
		job.setOutputKeyClass(Text.class);
		// Specifying the object type for reducer output Value
		job.setOutputValueClass(Text.class);
		// Specifying the input first argument as the input file for mapper
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// Specifying the input second argument as the output file for reducer
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	// Defined Mapper Class that extends Mapper Class
	public static class MapperForWordCount extends Mapper<Object,Text,Text,Text> {
		// Mapper Class with Object as Input Key and Text as Input Value and Text as Output Key and Text as Output Value
		public void map(Object key, Text value,Context con) throws IOException,InterruptedException {
			// Each line in the input file will be passed to the mapper
			String line = value.toString();
			// splitting based on ',' delimiter but restricting it if it is enclosed between ()
			String[] splits = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
			// splitting the date value from each line to capture the year
			String[] years = splits[0].split("/");
			// Check if there are 29 values after splitting the row, and check if there are no null values for the required 8 columns of interest
			if(splits.length == 29) {
				if((splits[0].length()>0) && (splits[2].length()>0) && (splits[3].length()>0) && (splits[10].length()>0) && (splits[11].length()>0) && (splits[12].length()>0) && (splits[13].length()>0) && (splits[14].length()>0) && (splits[15].length()>0) && (splits[16].length()>0) && (splits[17].length()>0) && (splits[24].length()>0)) {
					// Integer values below hold the values for each respective category and String holds those same values for supporting the Text format of output value
					int i = 1;
					String one = Integer.toString(i);
					int fatality1 = Integer.parseInt(splits[11])+Integer.parseInt(splits[13])+Integer.parseInt(splits[15])+Integer.parseInt(splits[17]);
					String fatality = Integer.toString(fatality1);
					int count11 = Integer.parseInt(splits[10]) + Integer.parseInt(splits[12]);
					String count1 = Integer.toString(count11);
					int count21 = Integer.parseInt(splits[11]) + Integer.parseInt(splits[13]);
					String count2 = Integer.toString(count21);
					int count31 = Integer.parseInt(splits[14]) + Integer.parseInt(splits[15]);
					String count3 = Integer.toString(count31);
					int count41 = Integer.parseInt(splits[16]) + Integer.parseInt(splits[17]);
					String count4 = Integer.toString(count41);
					// Using Context Object Write method to send the intermediate key value pairs to the partitioner class
					// Specific keys can be distinguished by using a tab delimiter to a unique string for each key
					con.write(new Text("date"+"\t"+splits[0]), new Text(one));
					con.write(new Text("borough"+"\t"+splits[2]),new Text(fatality));
					con.write(new Text("zip"+"\t"+splits[3]),new Text(fatality));
					con.write(new Text("vehicle"+"\t"+splits[24]),new Text(one));
					con.write(new Text("ppi"+"\t"+years[2]),new Text(count1));
					con.write(new Text("cci"+"\t"+years[2]),new Text(count2));
					con.write(new Text("cik"+"\t"+years[2]),new Text(count3));
					con.write(new Text("mik"+"\t"+years[2]),new Text(count4));
				}
			}
		}
	}
	
	// Defined Partitioner Class that extends Partitioner Class
	public static class PartitionerForWordCount extends Partitioner<Text,Text>
	{
		// Partitioner Class with Text as Input Key and Text as Input Value and Text as Output Key and Text as Output Value
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks)
		{
			// Assign number of reducers for each unique Key that is passed from Mapper
			String[] str = key.toString().split("\t");
			if(numReduceTasks == 0)
			{
				return 0;
			}
			if(str[0].equals("date"))
			{
				return 0;
			}
			else if(str[0].equals("borough"))
			{
				return 1 % numReduceTasks;
			}
			else if(str[0].equals("zip")) {
				return 2 % numReduceTasks;
			}
			else if(str[0].equals("vehicle")) {
				return 3 % numReduceTasks;
			}
			else if(str[0].equals("ppi")) {
				return 4 % numReduceTasks;
			}
			else if(str[0].equals("cci")) {
				return 5 % numReduceTasks;
			}
			else if(str[0].equals("cik")) {
				return 6 % numReduceTasks;
			}
			else if(str[0].equals("mik")) {
				return 7 % numReduceTasks;
			}
			return 0;
		}
	}
	
	// Reducer Class that extends Reducer Class
	public static class ReducerForWordCount extends Reducer<Text,Text,Text,Text> {
		public Integer maxCount = 0;
		public Text maxkey = new Text();
		// Reducer Class with Text as Input Key and Text as Input Value and Text as Output Key and Text as Output Value
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
			Integer count = 0;
			for (Text val : values)
			{
				// For each unique Key that is passed from Partitoner sum of the values is performed
				String parts[] = key.toString().split("\t");
				if (parts[0].equals("zip") || parts[0].equals("borough") || parts[0].equals("ppi")|| parts[0].equals("cci") || parts[0].equals("cik") || parts[0].equals("mik")) {
					count = count+Integer.parseInt(val.toString());
				}
				else if (parts[0].equals("date") || parts[0].equals("vehicle")) {
					count++;
				}
			}
			// Highest summed value will be assigned as maximum count and the Key will be set to the Output Key
			if(count > maxCount) {
				maxCount=count;
				maxkey.set(new Text(key.toString().split("\t")[1]));
			}
		}

		@Override
		public void cleanup(Context context) {
			try {
				context.write(new Text(maxkey), new Text(maxCount.toString()));
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}

