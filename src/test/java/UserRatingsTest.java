import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.apache.hadoop.shaded.org.hamcrest.CoreMatchers.is;
import static org.apache.hadoop.shaded.org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;


public class UserRatingsTest {
    private MapDriver<LongWritable, Text,Text,Text> mapDriver;
    private MapDriver<LongWritable,Text,Text,Text> mapDriver1;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    @Test
    public void MovieMapperTest() throws Exception{
       UserRatings.Movie_Mapper movieMapper = new UserRatings.Movie_Mapper();
       mapDriver=MapDriver.newMapDriver(movieMapper);
        MovieRatingTest am = new MovieRatingTest();
        am.movieMapperWithMultipleKeyAndValue(mapDriver);
    }

    @Test
    public void ratingMapperTest() throws IOException {
        UserRatings.UserRating_Mapper ratingMapper = new UserRatings.UserRating_Mapper();
        mapDriver1=MapDriver.newMapDriver(ratingMapper);
        String firstRecord="1,2,3.5";
        String secondRecord="2,4,4.5";
        int i=0;
        mapDriver1.withInput(new LongWritable(1),new Text(firstRecord));
        mapDriver1.withInput(new LongWritable(2),new Text(secondRecord));
        final List<Pair<Text,Text>> output = mapDriver1.run();
        final List<Pair<Text,Text>> sample= new ArrayList<Pair<Text, Text>>();
        sample.add(new Pair<Text, Text>( new Text("2"),new Text("1,3.5")));
        sample.add(new Pair<Text, Text>( new Text("4"),new Text("2,4.5")));
        assertThat("Size of mapper is not as expected",output.size(),is(2));
        for (Pair<Text,Text> values:output){
            if(i<sample.size()) {
                assertEquals("Output is as expected",values, sample.get(i));
                i++;
            }
        }
    }
    @Test
    public void userRatingReducerTest() throws IOException {
        UserRatings.User_Reducer reducer = new UserRatings.User_Reducer();
        reduceDriver=ReduceDriver.newReduceDriver(reducer);
        List<Text> values =new ArrayList<Text>();
        values.add(new Text("Toy Story (1995):"));
        values.add(new Text("2,4.5"));
        reduceDriver.withInput(new Text("1"),values);
        List<Pair<Text,Text>> output = reduceDriver.run();
        assertThat("Size of mapper is not as expected",output.size(),is(1));
        assertEquals("output is not as expected",output.get(0),new Pair<Text, Text>(new Text("2"),new Text("Toy Story (1995):\t4.5")));
    }
}

