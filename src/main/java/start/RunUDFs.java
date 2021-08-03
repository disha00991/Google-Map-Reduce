import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.util.Random;
import java.io.FileReader;

public class RunUDFs {
    
    // Reads user process metadata through json files and runs the UDFs on MapReduce Framework
    // as implemented by our Master/Worker files in this project
    public static  void main(String[] args){
        try {
            System.out.println("RUNNING WORDCOUNT USING MAPREDUCE (#WORKERS = 1)...");

            MapReduce wordCountInstance = new WordCount();
            JSONObject wordCountConfig = readJson("metadata/wordcountmetadata.json");
            Master masterInstanceForWordCount = new Master(wordCountConfig, wordCountInstance);        
            masterInstanceForWordCount.run();            
            System.out.println("***COMPLETED WORDCOUNT! Check docs/output/wordcount for results***");

            Thread.sleep(4000);
            //----------------------------------------------------------------------
            
            System.out.println("RUNNING DISTRIBUTED GREP USING MAPREDUCE (#WORKERS = 3)...");

            MapReduce distGrepInstance = new DistributedGrep();
            JSONObject distGrepConfig = readJson("metadata/distributedgrepmetadata.json");
            Master masterInstanceForDistGrep = new Master(distGrepConfig, distGrepInstance);
            masterInstanceForDistGrep.run();

            System.out.println("***COMPLETED DISTRIBUTED GREP! Check docs/output/distributedgrep for results***");
            Thread.sleep(4000);
            //----------------------------------------------------------------------
            
            System.out.println("RUNNING URL FREQUENCY USING MAPREDUCE (#WORKERS = 5)..");

            MapReduce urlfreqInstance = new URLFrequency();
            JSONObject urlfreqConfig = readJson("metadata/urlfrequencymetadata.json");
            Master masterInstanceForURLFreq = new Master(urlfreqConfig, urlfreqInstance);
            masterInstanceForURLFreq.run();

            System.out.println("***Completed URL Frequency! Check docs/output/urlfrequency for results***");

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // Reads a JSON file for config and returns it in the form of a JSON object
    private static JSONObject readJson(String filename){
        JSONObject jsonObj = new JSONObject(); ;
        try{
            Object obj = new JSONParser().parse(new FileReader(filename));
            jsonObj = (JSONObject) obj;
        } catch (Exception e){
            e.printStackTrace();
        }
        return jsonObj;
    }
}
