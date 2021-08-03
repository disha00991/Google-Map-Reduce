import java.util.ArrayList;

// Contains the mapreduce interface that the test cases implement
public interface MapReduce {
    public String getSeparator();
    public ArrayList<String[]> mapper(String line);
    public String reducer(ArrayList<String> values);
}
