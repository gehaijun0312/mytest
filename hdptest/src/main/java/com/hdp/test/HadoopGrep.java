package com.hdp.test;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.w3c.dom.Text;

public class HadoopGrep {
	public static class RegMapper extends MapReduceBase implements Mapper {
		
		private Pattern pattern;
		
		public void configure(JobConf job) {
			pattern = Pattern.compile(job.get( " mapred.mapper.regex " ));
		}
		
		public void map(WritableComparable key, Writable value, OutputCollector output, Reporter reporter)
				throws IOException {
			String text = ((Text) value).toString();
			Matcher matcher = pattern.matcher(text);
			if (matcher.find()) {
				output.collect(key, value);
			}
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void map(Object arg0, Object arg1, OutputCollector arg2, Reporter arg3) throws IOException {
			// TODO Auto-generated method stub
			
		}
	}
}
