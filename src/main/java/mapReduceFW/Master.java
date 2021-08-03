import org.json.simple.JSONObject;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.lang.management.ManagementFactory;

// Contains the implementation of the master.
// This is where the workers and the global synchronization barrier are created.
// For the mapper and reducer phases, we build separate processes.
public class Master {
    private int N;
    private String inputfile;
    private MapReduce udfObj;
    private String objectFile = "taskobject.ser";
    private int baseport;
    ServerSocket server;
    public boolean[] status = null;
    private static String phase;
    private static String intermediate;
    private static String output;
    private HashMap<Integer, String> lastHeard;
    public String[] processid = null;  //for fault tolerance
    //constructor
    public Master(JSONObject config, MapReduce udfObj){
        // using N processes
        this.N = Math.toIntExact((Long) config.get("workers"));
        this.inputfile = (String) config.get("inputfile");
        this.udfObj = udfObj;
        this.baseport = Math.toIntExact((Long) config.get("baseport"));
        this.intermediate = (String) config.get("intermediatedir");
        this.output = (String) config.get("outputdir");
    }

    // Create a worker process depending on the type of Phase , i.e Mapper or Reducer process
    public void createWorkerProcess(int id, String phase) {
        try {
            String input = new String("");
            String output = new String("");
            switch(phase) {
                case "mapper":
                    input = this.inputfile;
                    output = this.intermediate;
                    break;
                case "reducer":
                    input = this.intermediate;
                    output = this.output;
            }
            
            String javaHome = System.getProperty("java.home");
            String javaBin = javaHome +
                    File.separator + "bin" +
                    File.separator + "java";
            String classpath = System.getProperty("java.class.path");
            String className = "Worker";
//            System.out.println(javaBin + " -cp " + classpath +" " + className +" " + String.valueOf(id) +" " + inputfile +" " + Integer.toString(this.N) +" " + objectFile + " " + phase + " " + baseport);
            
            ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, className, String.valueOf(id), input, output, Integer.toString(this.N), objectFile, phase, String.valueOf(this.baseport));
            Process process = builder.inheritIO().start();
            String vmName = ManagementFactory.getRuntimeMXBean().getName();
            int p = vmName.indexOf("@");
                     
            String pid = vmName.substring(0, p);
            processid[id] = pid;
            SimpleDateFormat currdate = new SimpleDateFormat("yyMMddHHmmssZ");
            Date date = new Date();
            lastHeard.put(id, currdate.format(date));
        } catch (Exception e) {
            System.out.println("Problem in create worker process");
            // e.printStackTrace();
            System.out.println("Error while creating worker number: "+ id);
        }
    }

    //create a process
    private void createWorkers(String phase) {
        // Run a loop to create N processes
        for(int i=0; i < this.N; i++){
            this.createWorkerProcess(i, phase);
        }
    }

    private void runWorkerPhase(String phase) {
        status = new boolean[this.N];
        processid = new String[this.N];
        this.phase = phase;
        lastHeard = new HashMap<>();
        createWorkers(phase);
        globalBarrier();
    }

    // Initialize socket for hearbeat messages and work completion messages. Close after usage
    private void globalBarrier() {
        int count = 0;
        try{
            this.server = new ServerSocket(this.baseport);
            while (count < N) {
                SimpleDateFormat currdate = new SimpleDateFormat("yyMMddHHmmssZ");
                Date firstParsedDate = new Date();
                for(int i: lastHeard.keySet()){
                    if(!status[i]) {
                        Date secondParsedDate = currdate.parse(lastHeard.get(i));
                        long diff = firstParsedDate.getTime() - secondParsedDate.getTime() ;
//                            System.out.println("Difference: " + diff + " " + i);
                        if (diff > 2000) {
                            System.out.println("Worker Process with id: " + i + " died.");
                            createWorkerProcess(i, phase);
                            System.out.println("Worker Process with id: " + i + " respawned.");
                        }
                    }
                }

                Socket socket = null;
                socket = server.accept();
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());

                String message;
                try {
                    message = (String) ois.readObject();
                    String[] messageparts = message.split("\\s+");
                    String messageinfo = messageparts[0];
                    String workerPhase = messageparts[1];
                    int id =  Integer.parseInt(messageparts[2]);
                    if (workerPhase.equals(this.phase)) {
                        if (messageinfo.equals("Done") && !status[id]) {
                            count += 1;
                            status[id] = true;
                            ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                            oos.writeObject("Message Received Done: " + id);
                            lastHeard.remove(id);
                            oos.close();
                        } else if (messageinfo.equals("Alive")){
                            System.out.println("Received Heartbeat from " + id);
                            lastHeard.put(id, messageparts[3]);
                        } 
                    } else {
                        System.out.println("Some Error in global barrier");
                        System.out.println("Error Details: ");
                        System.out.println("Id: " + id);
                        System.out.println("Master phase: " + this.phase);
                        System.out.println("Worker phase: " + workerPhase);
                        System.out.println("Message Info: " + messageinfo);
                        System.out.println("Status of worker: " + status[id]);
                    }
                } catch (ClassNotFoundException e) {
                    System.out.println("Got disconnected here 1111");
                    // e.printStackTrace();
                }
                socket.close();
            }
            this.server.close();
        } catch (Exception e) {
            System.out.println("Got disconnected here");
            // e.printStackTrace();
        }
        System.out.println("Out of global barrier");
    }

    // Method for serialization and saving the state of object
    private void serializeObject() {
        try {
            FileOutputStream fileOut = new FileOutputStream(this.objectFile);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this.udfObj);
            out.close();
            fileOut.close();
        } catch ( Exception ee ){
            System.out.println("Got disconnected here 222");
            // ee.printStackTrace();
        }
    }

    void run() {        
        serializeObject();
        runWorkerPhase("mapper");
        runWorkerPhase("reducer");
    }
}
