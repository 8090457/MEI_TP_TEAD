package tead;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class HotelPriceAverage {

    // Mapper que recebe um objeto, um texto de entrada, e emite um par chave-valor (hotelId, preço médio)
    public static class HotelPriceMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        private final static DoubleWritable price = new DoubleWritable();
        private Text hotelId = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] fields = value.toString().split(";");
                hotelId.set(fields[0]); // hotel Id na posição 0
                price.set(Double.parseDouble(fields[16]) / Integer.parseInt(fields[10])); // divide o preço por  n.º de noites
                context.write(hotelId, price); // emite a saída
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) { // exceção para caso ocorra erro na conversão do tipo de dados ou na leitura do campo
                System.err.println("Erro: " + e.getMessage());
            }
        }
    }

    // Reducer recebe um par chave-valor (hotelId, lista de preços médios) e emite um par chave-valor (hotelId, preço médio)
    public static class HotelPriceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            try {
                double sum = 0;
                int count = 0;
                ArrayList<Double> priceList = new ArrayList<Double>(); // cria um ArrayList para armazenar os valores
                for (DoubleWritable val : values) {
                    double price = val.get();
                    sum += price;
                    count++;
                    priceList.add(price); // adiciona o preço à lista
                }
                Collections.sort(priceList, Collections.reverseOrder()); // ordena o ArrayList em ordem decrescente
                result.set(Math.round((sum / count) * 100.0) / 100.0); // cálculo da média e arredondamento para 2 casas decimais
                context.write(key, result);
            } catch (ArithmeticException e) { // exceção para caso ocorra erro na divisão por zero
                System.err.println("Erro: " + e.getMessage());
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(HotelPriceAverage.class);
        job.setMapperClass(HotelPriceMapper.class);
        job.setReducerClass(HotelPriceReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
