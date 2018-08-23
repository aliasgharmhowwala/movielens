import org.apache.hadoop.io.DoubleWritable;
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
public class UserMaxMinTest {
    private MapDriver<LongWritable, Text,Text, DoubleWritable> mapDriver;
    private ReduceDriver<Text, DoubleWritable, Text, Text> reduceDriver;
    @Test
    public void userRatingMaxMinMapperTest() throws Exception{
        UserMaxMin.UserRatings_Mapper userRatings_mapper = new UserMaxMin.UserRatings_Mapper();
        mapDriver=MapDriver.newMapDriver(userRatings_mapper);
        String firstRecord="1,2,3.5";
        String secondRecord="2,4,4.5";
        int i=0;
        mapDriver.withInput(new LongWritable(1),new Text(firstRecord));
        mapDriver.withInput(new LongWritable(2),new Text(secondRecord));
        final List<Pair<Text,DoubleWritable>> output = mapDriver.run();
        final List<Pair<Text,DoubleWritable>> sample= new ArrayList<Pair<Text, DoubleWritable>>();
        sample.add(new Pair<Text, DoubleWritable>( new Text("1"),new DoubleWritable(3.5)));
        sample.add(new Pair<Text, DoubleWritable>( new Text("2"),new DoubleWritable(4.5)));
        assertThat("Size of mapper is not as expected",output.size(),is(2));
        for (Pair<Text,DoubleWritable> values:output){
            if(i<sample.size()) {
                assertEquals("Output is as expected",values, sample.get(i));
                i++;
            }
        }
    }
    @Test
    public void userRatingMaxMinReducerTest() throws Exception{
        UserMaxMin.UserRatings_Reducer reducer = new UserMaxMin.UserRatings_Reducer();
        reduceDriver=ReduceDriver.newReduceDriver(reducer);
        List<DoubleWritable> values =new ArrayList<DoubleWritable>();
        values.add(new DoubleWritable(3.5));
        values.add(new DoubleWritable(4.5));
        reduceDriver.withInput(new Text("1"),values);
        List<Pair<Text,Text>> output = reduceDriver.run();
        assertThat("Size of mapper is not as expected",output.size(),is(1));
        assertEquals("Output is as expected",output.get(0),new Pair<Text, Text>(new Text("1"),new Text("4.5 3.5 4.0")));
    }

}
