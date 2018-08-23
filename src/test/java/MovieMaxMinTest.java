import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;
import java.util.*;
import static org.apache.hadoop.shaded.org.hamcrest.CoreMatchers.is;
import static org.apache.hadoop.shaded.org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class MovieMaxMinTest {
    private MapDriver<LongWritable, Text,Text, Text> mapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    @Test
    public void movieMaxMinMovieMapperTest() throws Exception{
        MovieMaxMin.Movie_Mapper movieMapper = new MovieMaxMin.Movie_Mapper();
        mapDriver=MapDriver.newMapDriver(movieMapper);
        MovieRatingTest am = new MovieRatingTest();
        am.movieMapperWithMultipleKeyAndValue(mapDriver);
    }
    @Test
    public void movieRatingMaxMinMapperTest() throws Exception{
        MovieMaxMin.Movie_Ratings_Mapper userRatings_mapper = new MovieMaxMin.Movie_Ratings_Mapper();
        mapDriver=MapDriver.newMapDriver(userRatings_mapper);
        String firstRecord="1,2,3.5";
        String secondRecord="2,4,4.5";
        int i=0;
        mapDriver.withInput(new LongWritable(1),new Text(firstRecord));
        mapDriver.withInput(new LongWritable(2),new Text(secondRecord));
        final List<Pair<Text,Text>> output = mapDriver.run();
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
    @Test
    public void movieMaxMinReducerTest() throws Exception{
        MovieMaxMin.Movie_Ratings_Reducer reducer = new MovieMaxMin.Movie_Ratings_Reducer();
        reduceDriver=ReduceDriver.newReduceDriver(reducer);
        List<Text> values =new ArrayList<Text>();
        values.add(new Text("Toy Story (1995):"));
        values.add(new Text("3.5"));
        values.add(new Text("4.5"));
        reduceDriver.withInput(new Text("1"),values);
        List<Pair<Text,Text>> output = reduceDriver.run();
        assertThat("Size of mapper is not as expected",output.size(),is(1));
        assertEquals("Output is as expected",output.get(0),new Pair<Text, Text>(new Text("Toy Story (1995):"),new Text("4.5 3.5 4.0")));
    }

}
