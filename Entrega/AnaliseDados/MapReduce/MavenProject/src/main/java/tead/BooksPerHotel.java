package tead;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BooksPerHotel {

  // Mapper que recebe um objeto, um texto de entrada, e emite um par chave-valor (hotelId, 1) que identifica uma reserva
  public static class HotelBooksMapper
    extends Mapper<Object, Text, Text, IntWritable> {

    private static final IntWritable one = new IntWritable(1);
    private final Text hotel = new Text();

    @Override
    public void map(
      Object key,
      Text value,
      Mapper<Object, Text, Text, IntWritable>.Context context
    ) throws IOException, InterruptedException {
      String[] splitValues = value.toString().split(";");

      String hotelId = splitValues[0];

      hotel.set(hotelId);
      context.write(hotel, one);
    }
  }

  // Reducer recebe um par chave-valor (hotelId, 1) e soma todas as linhas com a mesma chave
  public static class HotelBooksSumReducer
    extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(
      Text key,
      Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, IntWritable>.Context context
    ) throws IOException, InterruptedException {
      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    try {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "books per hotel");
      job.setJarByClass(BooksPerHotel.class);
      job.setMapperClass(HotelBooksMapper.class);
      job.setReducerClass(HotelBooksSumReducer.class);
      job.setCombinerClass(HotelBooksSumReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (IOException | InterruptedException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
