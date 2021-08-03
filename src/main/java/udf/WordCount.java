import java.io.Serializable;
import java.util.ArrayList;

// To count the words in a given text file
public class WordCount implements MapReduce, Serializable {
    @Override
    public String getSeparator() {
        return " ";
    }

    // Returns array list with the format [['word1','1'],['word2','1'] .... .]
    @Override
    public ArrayList<String[]> mapper(String line) {
        ArrayList<String[]> keyValue = new ArrayList<>();
        String[] words = line.replaceAll("\\p{P}", "").toLowerCase().split("\\s+");

        for(int i=0; i<words.length; i++){
            keyValue.add(new String[]{words[i], Integer.toString(1)});
        }
        return keyValue;
    }

    // Returns words total by using the size of array i.e size([['word1','1'],['word1','1'] .... .])
    public String reducer(ArrayList<String> words){
        return Integer.toString(words.size());
    }
}
