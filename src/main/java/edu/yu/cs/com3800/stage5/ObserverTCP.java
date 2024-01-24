package edu.yu.cs.com3800.stage5;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;

public class ObserverTCP extends Thread implements LoggingServer{
    ServerSocket server; 
    LeaderLock lock; 
    Logger logger; 
    public ObserverTCP(LeaderLock lock, int port){
        setDaemon(true);
        this.lock = lock;  
        port +=2; 
        try {
            this.server = new ServerSocket(port);
            this.logger = initializeLogging(ObserverTCP.class.getSimpleName()+ "-on-port-" + port); 
        } catch (IOException e) {
        } 

    }

    @Override
    public void run(){
        while(!Thread.currentThread().isInterrupted()){
            try {
                Socket socket = this.server.accept();
                ObjectInputStream fromLeader = new ObjectInputStream(socket.getInputStream());
                byte[] message = (byte[]) fromLeader.readObject(); 
                Message n = new Message(message);  
                this.logger.fine("received message");
                if(n.getMessageType() == MessageType.NEW_LEADER_GETTING_LAST_WORK){
                    synchronized(lock){
                        lock.ready = true; 
                        lock.notifyAll();
                    } 
                    this.logger.fine("The ready lock is set to: " + lock.ready); 
                    this.logger.fine("Now everyone can make requests like normal"); 
                }
            } catch (IOException | ClassNotFoundException e) {
                //this.logger.fine(e.getMessage()); 
                Thread.currentThread().interrupt(); 
            } 
        }
        this.logger.fine("TCP server for observer is closing"); 
    }
}
