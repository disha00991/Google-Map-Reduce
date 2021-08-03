import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

// Contains the implementation of the workers. 
// Both file reading and writing operations are handled here.
public class Worker {
    private static int workerId;
    private static int N;
    private static MapReduce workerObj;
    private static String workerPhase;
    private static String input;
    private static String output;
    private static int port;

    private WorkerHeartBeat heartBeatobj; 
    
    // Assign files to each worker process and start the heartbeat thread, 
    // depending on the phase mapper or reducer
    Worker(int id, String input, String output, int N, MapReduce obj, String workerPhase, int port){
        this.workerId = id;
        this.N = N;
        this.workerObj = obj;
        this.workerPhase = workerPhase;
        this.port = port;

        switch(workerPhase) {
            case "mapper":
                this.input = input;
                break;
            case "reducer": //case : reducer
                this.input = input + "/" + workerId + ".txt";
                break;
        }

        this.output = output + "/" + workerId + ".txt";

        heartBeatobj = new WorkerHeartBeat(workerId, port, workerPhase);
        heartBeatobj.start();
    }

    // Create File
    private void createFile(String path){
        try {
            System.out.println("the path here in create:"+ path);
            File f = new File(path);
            if (!f.exists()) {
                f.createNewFile();
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    // Return the file size in bytes
    private long getFileSizeInBytes(){
        long filesize = 0;

        try {
            Path path = Paths.get(this.input);
            filesize = Files.size(path);
        }
        catch( Exception e) {
            e.printStackTrace();
        }
        return filesize;
    }

    // Reads the chunk of the file allocated to the worker with the help of worker id
    // The file is equally divided in bytes amongst the workers to keep the load consistent
    private String readChunk() throws Exception{
        int filesize = (int) getFileSizeInBytes();
        int chunksize =  (int) Math.ceil(filesize*1.0/N);
        int startpos = this.workerId*chunksize;

        RandomAccessFile file = new RandomAccessFile(this.input, "r");
        file.seek(startpos);

        while (true && workerId > 0 && file.getFilePointer() < filesize){
            byte[] char_buffer = new byte[1];
            file.readFully(char_buffer);
            String ch = new String(char_buffer);
            if (ch.equals(workerObj.getSeparator())){
                break;
            }
        }

        int readsize = Math.min(chunksize, filesize - (int)file.getFilePointer());
        byte[] buffer = new byte[readsize];
        file.readFully(buffer);
        String chunk = new String(buffer);
        while (file.getFilePointer() < filesize){
            byte[] char_buffer = new byte[1];
            file.readFully(char_buffer);
            String ch = new String(char_buffer);
            if (ch.equals(workerObj.getSeparator())){
                break;
            }
            chunk += ch;
        }
        file.close();
        return chunk;
    }

    // The mapper phase is initiated for a particular worker
    // Worker reads from intermediate files to map,
    // and saves the results to intermediate files for use in the reducer.
    private void workAsMapper(){

        ArrayList<String[]> keyValuePair;

        try {
            // start the mapper phase
            System.out.println("Started Mapping Phase for Worker " + workerId);
            String chunk = readChunk();
            String[] readLines = chunk.split("\\r?\\n");
            if (readLines.length >0){
                createFile(this.output);
                PrintWriter outintermediateFile = new PrintWriter(new FileWriter(this.output));
                for(int j = 0; j<readLines.length; j++){
                    readLines[j] = readLines[j].trim();
                    if (readLines[j].length()>0) {
                        keyValuePair = workerObj.mapper(readLines[j]);
                        for (int i = 0; i < keyValuePair.size(); i++) {
                            outintermediateFile.println(keyValuePair.get(i)[0] + "," + keyValuePair.get(i)[1]);
                        }
                    }
                }
                outintermediateFile.close();
            }
            heartBeatobj.end();
            // only one process so after this we can start the reducer phase
            System.out.println("End Mapping Phase for Worker " + workerId);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // Summarizes the map output records with the same key by using hashmap,
    // and skips any bad records  
    private HashMap<String, ArrayList<String>> combiner(){
        HashMap<String, ArrayList<String>> keyValueHashMap = new HashMap<String, ArrayList<String>>();
        try {
            FileReader fr=new FileReader(this.input); //reads the file
            BufferedReader br=new BufferedReader(fr); //creates a buffering character input stream
            String line;
            while ((line=br.readLine()) != null){
                String[] splitLine = line.split(",");
                if (splitLine.length ==2){
                    ArrayList<String> emptyList = new ArrayList<String>();
                    keyValueHashMap.putIfAbsent(splitLine[0],emptyList);
                    emptyList = keyValueHashMap.get(splitLine[0]);
                    emptyList.add(splitLine[1]);
                    keyValueHashMap.put(splitLine[0], emptyList);
                }else {
                    System.out.println("Skipping Bad Record");
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        return keyValueHashMap;
    }

    // The reducer phase is initiated for a particular worker
    // Worker reads from intermediate files to reduce,
    // and saves the output to intermediate files.
    private void workAsReducer(){
        try{
            System.out.println("Started Combiner Phase for Worker " + workerId);
            HashMap<String, ArrayList<String>> combinerOutput =  combiner();

            createFile(this.output);
            System.out.println("Started Reducer Phase for Worker " + workerId);
            PrintWriter outputFilePointer = new PrintWriter(new FileWriter(this.output));
            for(String key: combinerOutput.keySet()){
                outputFilePointer.println(key + " " + workerObj.reducer(combinerOutput.get(key)));
            }
            outputFilePointer.close();
            heartBeatobj.end();
            System.out.println("End Reducing Phase for Worker " + workerId);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    // Sends message to Master 
    private void sendMessageToMaster(String message, int numTrials){
        
        try{
            Socket s = new Socket("localhost", this.port);
            ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
            String string;
            oos.writeObject(message + " " + workerPhase + " " + this.workerId);
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            string = (String) ois.readObject();
            if(!string.equals("Message Received Done: " + this.workerId ) && numTrials > 0){
                sendMessageToMaster(message, numTrials-1);
            }
            s.close();
        }catch (Exception e){
            // System.out.println("Do nothing here as indicates that worker's work is done");
            // e.printStackTrace();
        }
    }

    // Run worker 
    public void run(){
        switch(workerPhase) {
            case "mapper":
                workAsMapper();
                break;
            case "reducer":
                workAsReducer();
                break;
        }
        sendMessageToMaster("Done", 1);
    }

    public static void main (String[] args){

        int id = Integer.parseInt(args[0]); // worker ID
        String input = args[1]; // To measure chunk size in multiple-worker implementation
        String output = args[2]; // Output of worker
        int N = Integer.parseInt(args[3]); // To measure chunk size in multiple-worker implementation
        String objectFile = args[4]; 
        String workerPhase = args[5]; // Mapper or Reducer phase
        int port = Integer.parseInt(args[6]);

        try {
            FileInputStream fileIn = new FileInputStream(objectFile);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            MapReduce obj = (MapReduce) in.readObject();

            in.close();
            fileIn.close();

            Worker worker = new Worker(id, input, output, N, obj, workerPhase, port);
            worker.run();
        } 
        catch (IOException i) {
            i.printStackTrace();
            return;
        } 
        catch (ClassNotFoundException c) {
            System.out.println("Class Not Found");
            c.printStackTrace();
            return;
        }
    }
}