import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MovieRatings {
    private static String line="";
    private static String [] tokens=null;

    public static class Movie_Mapper extends Mapper<LongWritable,Text,Text,Text>{
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get()!=0) {
                line = value.toString();
                if (line.contains("\"")) {
                    Pattern pattern = Pattern.compile("((.+)(,)(\".*\")(,))");
                    Matcher match = pattern.matcher(line);
                    String movieID = "";
                    String movieName = "";
                    if (match.find()) {
                        movieID = match.group(2);
                        movieName = match.group(4)+":";
                    }
                    context.write(new Text(movieID), new Text(movieName));
                } else {
                    Pattern pattern = Pattern.compile("((.+)(,)(.*)(,))");
                    Matcher match = pattern.matcher(line);
                    String movieID = "";
                    String movieName = "";
                    if (match.find()) {
                        movieID = match.group(2);
                        movieName = match.group(4)+":";
                    }
                    context.write(new Text(movieID), new Text(movieName));

                }
            }

        }
    }
    public static class Rating_Mapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get()!=0){
                line=value.toString();
                tokens=line.split(",");
                String movieID=tokens[1];
                String ratings=tokens[2];
                context.write(new Text(movieID),new Text(ratings));
            }
        }
    }
    public static class Movie_Reducer extends Reducer<Text,Text,Text,Text>{
        String movieName="";
        int count=0;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text value : values) {
                if (value.toString().contains(":")){
                    Pattern pattern = Pattern.compile("((.*)(:))");
                    Matcher match =pattern.matcher(value.toString());
                    if (match.find()) {
                        movieName = match.group(2);
                    }

                }
                else {
                    count++;
                }
            }
            context.write(new Text(movieName),new Text(String.valueOf(count)));
            count=0;
            movieName="";

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job jb = Job.getInstance();
        jb.setJarByClass(MovieRatings.class);
        jb.setInputFormatClass(TextInputFormat.class);
        jb.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(jb,new Path(args[0]),TextInputFormat.class,MovieRatings.Movie_Mapper.class);
        MultipleInputs.addInputPath(jb,new Path(args[1]),TextInputFormat.class,MovieRatings.Rating_Mapper.class);
        jb.setReducerClass(MovieRatings.Movie_Reducer.class);
        jb.setOutputKeyClass(Text.class);
        jb.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(jb,new Path(args[2]));
        jb.waitForCompletion(true);
    }

}

