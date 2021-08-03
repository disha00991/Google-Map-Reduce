import java.io.Serializable;
import java.util.ArrayList;

// To count the URL access frequency in a given text file
public class URLFrequency implements MapReduce, Serializable {
    @Override
    public String getSeparator() {
        return "\n";
    }

    // Returns array list with the format [['URL1','1'],['URL2','1'] .... .]
    @Override
    public ArrayList<String[]> mapper(String input) {
        ArrayList<String[]> keyValue = new ArrayList<>();
        keyValue.add(new String[]{input, Integer.toString(1)});
        return keyValue;
    }

    // Returns URLs total by using the size of array i.e size([['URL1','1'],['URL1','1'] .... .])
    @Override
    public String reducer(ArrayList<String> urls) {
        return Integer.toString(urls.size());
    }
}
