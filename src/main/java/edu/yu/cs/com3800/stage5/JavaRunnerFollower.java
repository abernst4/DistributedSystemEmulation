package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.logging.Logger;

import edu.yu.cs.com3800.ElectionNotification;
import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperLeaderElection;
import edu.yu.cs.com3800.Message.MessageType;

public class JavaRunnerFollower extends Thread implements LoggingServer{
    JavaRunner runner; 
    ServerSocket serverSocket; 
    Logger logger; 
    int port; 
    boolean removed; 
    volatile boolean shutdown; 
    public Queue<byte[]> cache; 
    ZooKeeperPeerServerImpl server; 
    public JavaRunnerFollower(int port, ZooKeeperPeerServerImpl server) throws IOException {
        this.runner = new JavaRunner(); 
        this.setDaemon(true); 
        this.server = server; 
        this.port = port + 2; 
        this.logger = initializeLogging(JavaRunnerFollower.class.getSimpleName() + "-on-port-" + this.port); 
        this.cache = new ArrayDeque<>(); 
        //create serverSocket
        this.serverSocket = new ServerSocket(this.port); 
    }

    public void shutdown(){
        try {
            this.serverSocket.close();
            this.shutdown = true;
            //this is new
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            this.logger.fine(e.getMessage()); 
        }
    }

    @Override
    public void run(){
        while(!shutdown){
            try {
                this.logger.fine("we are about to wait for a socket connection"); 
                Socket socket = serverSocket.accept(); 
                ObjectInputStream fromLeader = new ObjectInputStream(socket.getInputStream());
                byte[] message = (byte[]) fromLeader.readObject(); 
                Message n = new Message(message); 
                
                this.logger.fine("recieved message " + n.toString());  

                if(n.getMessageType() == MessageType.NEW_LEADER_GETTING_LAST_WORK){
                    //serialize chache into content
                    ByteArrayOutputStream byteStream = new ByteArrayOutputStream(); 
                    ObjectOutputStream objectStream = new ObjectOutputStream(byteStream); 
                    objectStream.writeObject(this.cache);
                    byteStream.close();
                    objectStream.close();
                    var output = byteStream.toByteArray();

                    n = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK, output, "127.0.0.1", this.port, 
                                    "127.0.0.1", n.getSenderPort(), -1); 

                    ObjectOutputStream toLeader = new ObjectOutputStream(socket.getOutputStream());
                    message = n.getNetworkPayload(); 
                    toLeader.writeObject(message);
                    this.logger.fine("sending response "+ n.toString()); 

                    fromLeader.close(); 
                    toLeader.close(); 
                    continue; 
                }

                if(n.getMessageType() == MessageType.ELECTION){
                    ElectionNotification note = this.server.getElectionNotification(); 
                    byte[] content = ZooKeeperLeaderElection.buildMsgContent(note); 
                    n = new Message(MessageType.ELECTION, content, "127.0.0.1", this.port, "127.0.0.1", n.getSenderPort()); 
                    ObjectOutputStream toGateway = new ObjectOutputStream(socket.getOutputStream()); 
                    content = n.getNetworkPayload();
                    toGateway.writeObject(content);

                    fromLeader.close(); 
                    toGateway.close(); 
                    continue; 
                }


                try{
                    var input = new ByteArrayInputStream(n.getMessageContents()); 
                    var output = this.runner.compileAndRun(input).getBytes(); 
                    n = new Message(MessageType.COMPLETED_WORK, output, "127.0.0.1", this.port, 
                                "127.0.0.1", n.getSenderPort(), n.getRequestID()); 
                }catch(Exception e){
                    var string = e.getMessage() + "\n" + Util.getStackTrace(e); 
                    var output = string.getBytes(); 
                    n = new Message(MessageType.COMPLETED_WORK, output, "127.0.0.1", this.port, 
                                "127.0.0.1", n.getSenderPort(), n.getRequestID(), true); 
                }

                message = n.getNetworkPayload(); 

                this.cache.add(message); 
                //TESTING PURPOSES 
                //if(this.port == 8002 && n.getRequestID() == 0)Thread.sleep(100000);
                
                ObjectOutputStream toLeader = new ObjectOutputStream(socket.getOutputStream());
                toLeader.writeObject(message);
                this.logger.fine("sending response "+ n.toString()); 

                fromLeader.close(); 
                toLeader.close(); 
                
            } catch (IOException | ReflectiveOperationException e) {
                this.logger.fine("Error caught: " + e.getMessage()); 
            } 

            
        }
        this.logger.fine("Is the shutdown variable still false == " + shutdown); 
        this.logger.fine("Java runner is exiting"); 
    }
}