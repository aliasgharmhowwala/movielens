import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.apache.hadoop.shaded.org.hamcrest.CoreMatchers.is;
import static org.apache.hadoop.shaded.org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import org.junit.*;



public class MovieRatingTest {
    private MapDriver<LongWritable, Text,Text,Text> mapDriver;
    private MapDriver<LongWritable,Text,Text,Text> mapDriver1;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Test
    public void movieMapperTest() throws Exception
    {
            final MovieRatings.Movie_Mapper movieMapper = new MovieRatings.Movie_Mapper();
            mapDriver = MapDriver.newMapDriver(movieMapper);
            this.movieMapperWithMultipleKeyAndValue(mapDriver);
    }
    @Test
    public void ratingMapperTest() throws Exception
    {
        final MovieRatings.Rating_Mapper ratingsMapper = new MovieRatings.Rating_Mapper();
        mapDriver1 = MapDriver.newMapDriver(ratingsMapper);
        this.ratingMapperWithMultipleKeyAndValue(mapDriver1);

    }
    @Test
    public void movieReduceTest() throws Exception
    {
        final MovieRatings.Movie_Reducer movieReducer = new MovieRatings.Movie_Reducer();
        reduceDriver = ReduceDriver.newReduceDriver(movieReducer);
        this.reducer(reduceDriver);
    }


    public void movieMapperWithMultipleKeyAndValue(MapDriver mapper2) throws Exception {
        String firstRecord="1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy";
        String secondRecord="2,God Father(1997),Adventure|Animation|Children|Comedy|Fantasy";
        int i=0;
        mapper2.withInput(new LongWritable(1),new Text(firstRecord));
        mapper2.withInput(new LongWritable(2),new Text(secondRecord));
        final List<Pair<Text,Text>> output = mapper2.run();
        final List<Pair<Text,Text>> sample= new ArrayList<Pair<Text, Text>>();
        sample.add(new Pair<Text, Text>( new Text("1"),new Text("Toy Story (1995):")));
        sample.add(new Pair<Text, Text>( new Text("2"),new Text("God Father(1997):")));
        assertThat("Size of mapper is not as expected",output.size(),is(2));
        for (Pair<Text,Text> values:output){
            if(i<sample.size()) {
                assertEquals("Output is as expected",values, sample.get(i));
                i++;
            }
        }
    }
    public void ratingMapperWithMultipleKeyAndValue(MapDriver mapper2) throws Exception {
        String firstRecord="1,2,3.5";
        String secondRecord="2,4,4.5";
        int i=0;
        mapper2.withInput(new LongWritable(1),new Text(firstRecord));
        mapper2.withInput(new LongWritable(2),new Text(secondRecord));
        final List<Pair<Text,Text>> output = mapper2.run();
        final List<Pair<Text,Text>> sample= new ArrayList<Pair<Text, Text>>();
        sample.add(new Pair<Text, Text>( new Text("2"),new Text("3.5")));
        sample.add(new Pair<Text, Text>( new Text("4"),new Text("4.5")));
        assertThat("Size of mapper is not as expected",output.size(),is(2));
        for (Pair<Text,Text> values:output){
            if(i<sample.size()) {
                assertEquals("Output is as expected",values, sample.get(i));
                i++;
            }
        }
    }

    public void reducer(ReduceDriver reducer1) throws Exception {
        String key = "1";
        List<Text> values =new ArrayList<Text>();
        values.add(new Text("3.5"));
        values.add(new Text("4.5"));
        reducer1.withInput(new Text("1"),values);
        List<Pair<Text,Text>> output = reducer1.run();
        assertThat("Size of mapper is not as expected",output.size(),is(1));
        assertEquals("Output is as expected",output.get(0),new Pair<Text, Text>(new Text("1"),new Text("3.5")));
    }



}
