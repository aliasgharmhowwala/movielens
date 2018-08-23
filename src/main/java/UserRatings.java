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
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserRatings {
    private static String line="";
    private static String [] tokens=null;
    private static int firstItr=0;
    private static int secondItr=0;
    public static class UserRating_Mapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get()!=0){
                line=value.toString();
                tokens=line.split(",");
                String userID=tokens[0];
                String movieID=tokens[1];
                String ratings=tokens[2];
                context.write(new Text(movieID),new Text(userID+","+ratings));
            }
        }
    }
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
    public static class User_Reducer extends Reducer<Text,Text,Text,Text> {
        String movieName="";
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList <String> userID= new ArrayList();
            ArrayList <String> ratings= new ArrayList();
            for (Text value : values) {
                if(value.toString().contains(":")){
                    movieName=value.toString();
                }
                else{
                    tokens=value.toString().split(",");
                    userID.add(tokens[0]);
                    ratings.add(tokens[1]);
                }
            }
            for (int i=0;i<userID.size();i++) {
                context.write(new Text(userID.get(i)), new Text(movieName + "\t" + ratings.get(i)));
            }
            movieName="";
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job jb = Job.getInstance();
        jb.setJarByClass(UserRatings.class);
        jb.setInputFormatClass(TextInputFormat.class);
        jb.setOutputFormatClass(TextOutputFormat.class);
        MultipleInputs.addInputPath(jb,new Path(args[0]),TextInputFormat.class,UserRatings.UserRating_Mapper.class);
        MultipleInputs.addInputPath(jb,new Path(args[1]),TextInputFormat.class,UserRatings.Movie_Mapper.class);
        jb.setReducerClass(UserRatings.User_Reducer.class);
        jb.setOutputKeyClass(Text.class);
        jb.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(jb,new Path(args[2]));
        jb.waitForCompletion(true);
    }
}

