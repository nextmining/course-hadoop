package com.nextmining.hadoop.mapreduce;

import com.nextmining.hadoop.util.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * This class is a driver to filter the records by the given condtions.
 * 
 * @author Younggue Bae
 */
public class FilterJob extends AbstractJob {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new FilterJob(), args);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		addOption("input", "i", "Path to job input directory.", true);
		addOption("output", "o", "The directory pathname for output.", true);
		addOption("delimiter", "d", "Delimiter(The default is \\t)", false);
		addOption("condition", "c", "The filter condition.(ex.[(column_index operator condition_value),...], operators: eq, ne, gt, lt, ge, le, in", true);
		//Usage: [(0 eq '게임'),(1 gt 50),(3 in '모바일,웹')]
		
		parseArguments(args);

		String inputs = getOption("input");
		String[] input = inputs.split(",");
		String output = getOption("output");
		String delimiter = getOption("delimiter", "\t");
		String condition = getOption("condition");
		
    if (delimiter.equals("\\t")) {
    	delimiter = "\t";
    }

		Configuration conf = getConf();
		conf.set("delimiter", delimiter);
		conf.set("condition", condition);

		logger.info("delimiter == " + delimiter);
		logger.info("condition == " + condition);

		Path[] inputPath = new Path[input.length];
		for (int i = 0; i < input.length; i++) {
			inputPath[i] = new Path(input[i].trim());
		}
		Path outputPath = new Path(output);

		Job job = Job.getInstance(conf);

		job.setJobName(HadoopUtil.getCustomJobName(getClass().getSimpleName(), job, FilterMapper.class, Reducer.class));
		job.setJarByClass(FilterJob.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FilterMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setReducerClass(FilterReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		HadoopUtil.delete(conf, outputPath);

		job.waitForCompletion(true);

		return 0;
	}
	
	@SuppressWarnings("rawtypes")
	public static class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		private String delimiter = "\t";
		private List<FilterCondition> conditions;

		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);

			Configuration conf = context.getConfiguration();

			delimiter = conf.get("delimiter", "\t");
			conditions = convertConditions(conf.get("condition"));
		}
		
		private List<FilterCondition> convertConditions(String strCondition) {
			List<FilterCondition> conditions = new ArrayList<FilterCondition>();
			
			String[] strConds = strCondition.split("\\)\\,");
			for (String strCond : strConds) {
				strCond = strCond.replaceAll("\\[\\(", "").replaceAll("\\(", "").replaceAll("\\)\\]", "");
				String[] field = strCond.split("\\s");
				
				int columnIndex = Integer.parseInt(field[0]);
				String operator = field[1];
				String strValue = field[2];
				if ((strValue.startsWith("\'") && strValue.endsWith("\'")) || 
						(strValue.startsWith("\"") && strValue.endsWith("\""))) {
					strValue = strValue.replaceAll("\'", "").replaceAll("\"", "");
					FilterCondition<String> condition = new FilterCondition<String>();
					condition.setColumnIndex(columnIndex);
					condition.setOperator(operator.toUpperCase());
					condition.setValue(strValue);
					
					conditions.add(condition);
				}
				else {
					FilterCondition<Double> condition = new FilterCondition<Double>();
					condition.setColumnIndex(columnIndex);
					condition.setOperator(operator.toUpperCase());
					condition.setValue(Double.parseDouble(strValue));
					
					conditions.add(condition);
				}
			}
			
			System.out.println("conditions == " + conditions);
			
			return conditions;
		}
		
		private boolean satisfyConditions(String[] field, List<FilterCondition> conditions) {
			
			for (FilterCondition condition : conditions) {
				int columnIndex = condition.getColumnIndex();
				String strFieldValue = field[columnIndex];
				String operator = condition.getOperator();
				Object objCondValue = condition.getValue();
				
				if (objCondValue instanceof String) {
					String fieldValue = strFieldValue.trim();
					String condValue = ((String) objCondValue).trim();
					if (operator.equalsIgnoreCase(FilterCondition.EQ)) {
						if (fieldValue.equals(condValue)) {
							continue;
						}
						else {
							return false;
						}
					}
					else if (operator.equalsIgnoreCase(FilterCondition.NE)) {
						if (!fieldValue.equals(condValue)) {
							continue;
						}
						else {
							return false;
						}
					}
					else if (operator.equalsIgnoreCase(FilterCondition.GT)) {
					}
					else if (operator.equalsIgnoreCase(FilterCondition.LT)) {
					}					
					else if (operator.equalsIgnoreCase(FilterCondition.GE)) {
					}
					else if (operator.equalsIgnoreCase(FilterCondition.LE)) {
					}
					else if (operator.equalsIgnoreCase(FilterCondition.IN)) {
						List<String> condValueList = Arrays.asList(condValue.split(","));
						if (condValueList.contains(fieldValue)) {
							continue;
						}
						else {
							return false;
						}
					}
				}
				else if (objCondValue instanceof Double) {
					double fieldValue = Double.parseDouble(strFieldValue.trim());
					double condValue = (Double) objCondValue;
					if (operator.equalsIgnoreCase(FilterCondition.EQ)) {
						if (fieldValue == condValue) {
							continue;
						}
						else {
							return false;
						}
					}
					else if (operator.equalsIgnoreCase(FilterCondition.NE)) {
						if (fieldValue != condValue) {
							continue;
						}
						else {
							return false;
						}
					}
					else if (operator.equalsIgnoreCase(FilterCondition.GT)) {
						if (fieldValue > condValue) {
							continue;
						}
						else {
							return false;
						}
					}
					else if (operator.equalsIgnoreCase(FilterCondition.LT)) {
						if (fieldValue < condValue) {
							continue;
						}
						else {
							return false;
						}
					}					
					else if (operator.equalsIgnoreCase(FilterCondition.GE)) {
						if (fieldValue >= condValue) {
							continue;
						}
						else {
							return false;
						}
					}
					else if (operator.equalsIgnoreCase(FilterCondition.LE)) {
						if (fieldValue <= condValue) {
							continue;
						}
						else {
							return false;
						}
					}
					else if (operator.equalsIgnoreCase(FilterCondition.IN)) {
					}
				}
			}
			
			return true;
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] field = record.split(delimiter);

			if (this.satisfyConditions(field, conditions)) {
				context.write(NullWritable.get(), value);
			}
		}
	}

	public static class FilterReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		@Override
		protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			
			for (Text val : values) {
				context.write(NullWritable.get(), val);
			}
		}

	}
	
	static class FilterCondition<T> {
		private int columnIndex;
		private String operator;
		private T value;
		
		/* operater */
		public static final String EQ = "EQ";
		public static final String NE = "NE";
		public static final String GT = "GT";
		public static final String LT = "LT";
		public static final String GE = "GE";
		public static final String LE = "LE";
		public static final String IN = "IN";
		
		public FilterCondition() { }
		
		public FilterCondition(int columnIndex, String operator, T value) {
			this.columnIndex = columnIndex;
			this.operator = operator;
			this.value = value;
		}

		public int getColumnIndex() {
			return columnIndex;
		}

		public void setColumnIndex(int columnIndex) {
			this.columnIndex = columnIndex;
		}

		public String getOperator() {
			return operator;
		}

		public void setOperator(String operator) {
			this.operator = operator;
		}

		public T getValue() {
			return value;
		}

		public void setValue(T value) {
			this.value = value;
		}
		
		public String toString() {
			return "(" + columnIndex + " " + operator + " " + value + ")";
		}
		
	}

}
