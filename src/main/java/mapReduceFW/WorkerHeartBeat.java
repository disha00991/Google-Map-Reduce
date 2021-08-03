import java.util.*;
import java.net.Socket;
import java.net.ConnectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;

public class WorkerHeartBeat extends Thread {
        private int workerId;
        private int masterport;
        private String workerPhase;
        private boolean status = true;
        private Socket s;

    public WorkerHeartBeat(int workerId, int masterport, String workerPhase) {
            this.masterport = masterport;
            this.workerId = workerId;
            this.workerPhase = workerPhase;
        }

        // Publishing a heartbeat every 2 seconds
        @Override
        public void run() {
            while (status) {

                try{
                    Thread.sleep(2000);
                    this.s = new Socket("localhost", this.masterport);
                    ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                    SimpleDateFormat cur_date = new SimpleDateFormat("yyMMddHHmmssZ");
                    System.out.println("HeartBeat sent to Master from worker: " + workerId);
                    Date date = new Date();
                    oos.writeObject( "Alive"+ " " + workerPhase + " " + this.workerId+ " " + cur_date.format(date));
                    oos.close();
                    s.close();
                    s = null;

                }
                catch (ConnectException ce){
                    // Do nothing as this arises when the thread wakes up but the work is complete
                }
                catch (Exception e){
                    // e.printStackTrace();
                }
        }
    }

    // Called by the key worker process to show that the worker's job is complete
    // There is no need to send the heartbeat
    public void end(){
            this.status = false;
    }
}


