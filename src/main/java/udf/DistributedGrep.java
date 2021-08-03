import java.io.Serializable;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// To find a specific pattern in an input text file
public class DistributedGrep implements MapReduce, Serializable {
    @Override
    public String getSeparator() {
        return " ";
    }
    // Returns an array list showing the number of times the word "distributed"
    // occured in a line [['distributed' ,1], ['distributed' ,5], ['distributed' ,0], .... .]
    @Override
    public ArrayList<String[]> mapper(String line) {
        ArrayList<String[]> keyValuePair = new ArrayList<>();
        Matcher matcher = Pattern.compile("distributed", Pattern.CASE_INSENSITIVE)
                .matcher(line);
        int count = 0;
        while (matcher.find()) count += 1;
        keyValuePair.add(new String[]{"distributed", Integer.toString(count)});
        return keyValuePair;
    }

    // Sums the total number of times the word "distributed" occured in the intermediate file
    @Override
    public String reducer(ArrayList<String> values) {
        int sum = 0;
        for(int i=0; i<values.size(); i++){
            sum += Integer.parseInt(values.get(i));
        }
        return Integer.toString(sum);
    }
}
