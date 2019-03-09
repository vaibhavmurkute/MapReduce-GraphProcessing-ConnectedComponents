import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

/* 
	@Author: Vaibhav Murkute
	Project: Map-Reduce - Group Connected Components
	Date: 03/04/2019
	
*/


class Vertex implements Writable {
    public short tag;                 // 0 for a graph vertex, 1 for a group number
    public long group;                // the group where this vertex belongs to
    public long VID;                  // the vertex ID
    public Vector<Long> adjacent = new Vector<Long>();     // the vertex neighbors
	public int vector_size = 0;
    
	Vertex(){}
	
	Vertex(short t, long grp, long id, Vector<Long> v_adj){
		tag = t;
		group = grp;
		VID = id;
		adjacent = v_adj;
		vector_size = v_adj.size();
	}
	
	Vertex(short t, long grp){
		tag = t;
		group = grp;
	}
	
	public void write ( DataOutput out ) throws IOException {
		out.writeShort(tag);
		out.writeLong(group);
		out.writeLong(VID);
		out.writeInt(vector_size);
		for(int i=0; i < vector_size; i++){
			out.writeLong(adjacent.get(i));
		}
   	 }

    	public void readFields (DataInput in ) throws IOException {
        	tag= in.readShort();
		group = in.readLong();
		VID = in.readLong();
		vector_size = in.readInt();
		adjacent = new Vector<Long>();
		for(int k=0; k < vector_size; k++){
			adjacent.add(in.readLong());
		}
    	}
/*
	@Override
    public String toString () {
		StringBuilder sb = new StringBuilder();
		//sb.append(VID);
		//sb.append(",");
		for(Long v : adjacent){
			sb.append(Long.toString(v));
			sb.append(",");
		} 
		sb.deleteCharAt(sb.length()-1);
		return String.valueOf(sb.toString()); 
	}
*/

}

public class Graph {

    /* ... */
	public static class VertexMapper extends Mapper<Object,Text,LongWritable,Vertex> {
		
		@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException {
								
		    	Scanner s = new Scanner(value.toString()).useDelimiter(",");
			Vector<Long> v_adj = new Vector<Long>();
				
		    	long v_id = s.nextLong();
			while(s.hasNextLong()){
				v_adj.add(s.nextLong());
			}
		
		/*	String[] values = value.toString().split(",");
			v_adj.clear();
			long v_id = Long.parseLong(values[0]);
			for(int i=1; i < values.length; i++){
				v_adj.add(Long.parseLong(values[i]));
			}
		*/
		    
			context.write(new LongWritable(v_id),new Vertex((short)0, v_id, v_id, v_adj));
		    	s.close();
		}
    }
	
	public static class VertexReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            
		for (Vertex v: values) {
                	context.write(key, new Vertex(v.tag, v.group, v.VID, v.adjacent));
            	};
            
        }
    }
	
	public static class VertexGroupMapper extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
		@Override
		public void map ( LongWritable key, Vertex value, Context context )
				throws IOException, InterruptedException {

			context.write(new LongWritable(value.VID), value);

			for(long v : value.adjacent){
				context.write(new LongWritable(v), new Vertex((short)1, value.group));
			}	
		}
    }

	public static class VertexGroupReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> { 
		@Override
		public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
				   throws IOException, InterruptedException {
		    
			long min = Long.MAX_VALUE;
			Vertex vert = new Vertex();

			for (Vertex v: values) {
				if(v.tag == 0){
					vert = new Vertex(v.tag,v.group,v.VID,v.adjacent);
				}
				if (v.group < min){
					min = v.group;
				}
			}
			context.write(new LongWritable(min),new Vertex((short)0,min,vert.VID,vert.adjacent));
		}
	}

	public static class CounterMapper extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> {
		@Override
		public void map ( LongWritable key, Vertex value, Context context )
				throws IOException, InterruptedException {
		    
			context.write(new LongWritable(value.group),new LongWritable(1));
		}
	}		

	public static class CounterReducer extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
		@Override
		public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
				   throws IOException, InterruptedException {
			long count = 0L;
			for (LongWritable v : values){
				count += (v.get());
			}
				
			context.write(key,new LongWritable(count));
		}
	}

    public static void main ( String[] args ) throws Exception {
	
	boolean status = false;

	Job job1 = Job.getInstance();
        job1.setJobName("VertexmapperJob");
        job1.setJarByClass(Graph.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
	job1.setMapperClass(VertexMapper.class);
	job1.setReducerClass(VertexReducer.class);
	job1.setInputFormatClass(TextInputFormat.class);  
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
	FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1,new Path(args[1]+"/f0"));
		
	status = job1.waitForCompletion(true);

	Path in_path =new Path(args[1]+"/f0");
	Path out_path =null;
	
	if(status){
		for( int i=1;i<=5;i++){
			out_path = new Path(args[1]+"/f"+i);
			Job job2 = Job.getInstance();
			job2.setJobName("VertexGroupingJob");
			job2.setJarByClass(Graph.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Vertex.class);
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Vertex.class);
			job2.setMapperClass(VertexGroupMapper.class);
			job2.setReducerClass(VertexGroupReducer.class);
			job2.setInputFormatClass(SequenceFileInputFormat.class);
			job2.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.setInputPaths(job2, in_path);
			FileOutputFormat.setOutputPath(job2,out_path);
			in_path=out_path;
			
			status = job2.waitForCompletion(true);
		}
	}
	
	if(status){		
		Job job3 = Job.getInstance();
		job3.setJobName("CounterJob");
		job3.setJarByClass(Graph.class);
		job3.setOutputKeyClass(LongWritable.class);
		job3.setOutputValueClass(LongWritable.class);
		job3.setMapOutputKeyClass(LongWritable.class);
        	job3.setMapOutputValueClass(LongWritable.class);
		job3.setMapperClass(CounterMapper.class);
		job3.setReducerClass(CounterReducer.class);
		job3.setInputFormatClass(SequenceFileInputFormat.class);
        	job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job3, new Path(args[1]+"/f5"));
		FileOutputFormat.setOutputPath(job3,new Path(args[2]));
		status = job3.waitForCompletion(true);
	}

    }
}
