package tead;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class NightsPerOriginAverage {

  public static class NightsPerOriginMapper
      extends Mapper<Object, Text, Text, DoubleWritable> {

    private final Text country = new Text();

    @Override
    public void map(
        Object key,
        Text value,
        Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
      String[] splitValues = value.toString().split(";");

      String countryStr = splitValues[2];
      String nightsStr = splitValues[10];
      Double nights = Double.parseDouble(nightsStr);

      country.set(countryStr);
      context.write(country, new DoubleWritable(nights));
    }
  }

  public static class NightsPerOriginReducer
      extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable result = new DoubleWritable();

    @Override
    protected void reduce(
        Text key,
        Iterable<DoubleWritable> values,
        Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
      double sum = 0;
      int count = 0;
      ArrayList<Double> priceList = new ArrayList<Double>();

      for (DoubleWritable val : values) {
        double nights = val.get();
        sum += nights;
        count++;
        priceList.add(nights); // adiciona o preço à lista
      }
      
      result.set(Math.round((sum / count) * 100.0) / 100.0);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    try {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "average nights per origin");
      job.setJarByClass(NightsPerOriginAverage.class);
      job.setMapperClass(NightsPerOriginMapper.class);
      job.setReducerClass(NightsPerOriginReducer.class);
      job.setCombinerClass(NightsPerOriginReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(DoubleWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
