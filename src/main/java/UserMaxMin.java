import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class UserMaxMin{
    private static String line="";
    private static String [] tokens=null;
    private static int firstItr=0;
    private static int secondItr=0;
    public static class UserRatings_Mapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get()!=0){
                line=value.toString();
                tokens=line.split(",");
                String userID=tokens[0];
                Double ratings=Double.parseDouble(tokens[2]);
                context.write(new Text(userID),new DoubleWritable(ratings));
            }
        }
    }
    public static class UserRatings_Reducer extends Reducer<Text,DoubleWritable,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            String userID="";
            String maxRatings="";
            String minRatings="";
            double ratings=0;
            int count=0;
            double max=0,min=99999999,sum=0.0,average=0.0;
            for (DoubleWritable value:values){
                ratings=value.get();
                if(ratings>max){
                    maxRatings=value.toString();
                    max=ratings;
                }

                if (ratings<min){
                    minRatings=value.toString();
                    min=ratings;
                }
                sum+=ratings;
                count++;
            }
            average=sum/count;
            context.write(key,new Text(maxRatings+" "+minRatings+" "+String.valueOf(average)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job jb = Job.getInstance();
        jb.setJarByClass(UserMaxMin.class);
        jb.setInputFormatClass(TextInputFormat.class);
        jb.setOutputFormatClass(TextOutputFormat.class);
        jb.setMapperClass(UserMaxMin.UserRatings_Mapper.class);
        jb.setReducerClass(UserMaxMin.UserRatings_Reducer.class);
        jb.setMapOutputValueClass(DoubleWritable.class);
        jb.setOutputKeyClass(Text.class);
        jb.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(jb,new Path(args[0]));
        FileOutputFormat.setOutputPath(jb,new Path(args[1]));
        jb.waitForCompletion(true);
    }
}
