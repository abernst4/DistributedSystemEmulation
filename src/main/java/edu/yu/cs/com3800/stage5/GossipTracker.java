package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;

public class GossipTracker extends Thread implements LoggingServer{
    //this number will have to change 
    static final int Gossip = 1700; //was 3000
    static final int Fail = Gossip * 10; 
    static final int Cleanup = Fail * 2; 


    int gossipPort; 
    HttpServer httpServer; 
    Map<Long, Long> myGossipMap; 
    Map<Long, Long> TimeLastUpdatedMap;
    Map<Integer, Long> portToId; 
    LinkedBlockingQueue<Message> incomingMessages; 
    List<InetSocketAddress> ports; 
    long myId; 
    Map<Long, InetSocketAddress> peerMap; //to remove from broadcast list
    ZooKeeperPeerServerImpl server; 
    Random random;  
    ScheduledExecutorService executor;
    volatile long heartbeat; 
    volatile boolean shutdown; 
    ExecutorService cacheExec; 
    Logger verboseLog; 
    Logger shortLog; 
    //I need to serialize this class so that it can be turned into a byte array 
    public GossipTracker(LinkedBlockingQueue<Message> incomingMessages, Map<Long, InetSocketAddress> peerMap, long myId, ZooKeeperPeerServerImpl server){
        this.setDaemon(true);
        this.incomingMessages = incomingMessages; 
        this.server = server; 
        this.peerMap = peerMap; 
        this.TimeLastUpdatedMap = new HashMap<>(); 
        this.myId = myId; 
        this.cacheExec = Executors.newCachedThreadPool(); 
        this.executor = Executors.newScheduledThreadPool(1); 
        this.random = new Random(); 
        //let's make the list so that we can send random pulses everywhere and we can check if it exists
        ports = new ArrayList<>(); 
        for(InetSocketAddress entry: peerMap.values()){
            ports.add(entry); 
        }

        long creationTime = System.currentTimeMillis();
        this.myGossipMap = new ConcurrentHashMap<>(); 
        for(long id: peerMap.keySet()){
            this.TimeLastUpdatedMap.put(id, creationTime); 
            this.myGossipMap.put(id, 0L); 
        }

        this.portToId = new HashMap<>(); 
        for(Map.Entry<Long, InetSocketAddress> entry: peerMap.entrySet()){
            portToId.put(entry.getValue().getPort(),entry.getKey()); 
        }

        this.gossipPort = this.server.getAddress().getPort();
        int httpServerPort = server.getUdpPort() + 4; 
        try {
            this.verboseLog = initializeLogging("VerboseGossipTracking-on-port-" + gossipPort);
            this.shortLog = initializeLogging("AbridgedGossipTracking-on-port-" + gossipPort);
            this.httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", httpServerPort), 0); 
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void shutdown(){
        try{
            this.executor.shutdown();
            this.executor.awaitTermination(10, TimeUnit.MINUTES); 
            this.cacheExec.shutdown();
            this.cacheExec.awaitTermination(10, TimeUnit.MINUTES); 
        }catch(Exception e){}
        this.httpServer.stop(0);
        this.shutdown = true; 
    }
  
    public void run(){

        this.httpServer.createContext("/verbose", new VerboseHandler()); 
        this.httpServer.createContext("/summary", new AbridgedHandler()); 
        this.httpServer.start();

        //send a request to a random node 
        this.executor.scheduleAtFixedRate(() ->{
            try{
                Map<Long, Long> copy = new ConcurrentHashMap<>(this.myGossipMap); 
                //HeartBeat NewBeat = new HeartBeat(myId, heartbeat++, System.currentTimeMillis()); 
                copy.put(myId, heartbeat++); 
                //HeartBeatWrapper wrapper = new HeartBeatWrapper(copy, myId); 
                byte[] content = serializeHashMap(copy); 
                int index; 
                synchronized(ports){
                    index = this.random.nextInt(this.ports.size()); 
                }
                this.server.sendMessage(MessageType.GOSSIP, content, this.ports.get(index));
            }catch(Exception e){
                this.verboseLog.fine(e.getMessage());
            }
        }, 0, Gossip, TimeUnit.MILLISECONDS); 
        
        int wait = 200; 
        int mostWait = 60000; 
        while(!this.shutdown){
            try {
                Message n = this.incomingMessages.poll(wait, TimeUnit.MILLISECONDS);
                if(n != null){
                    Runnable task = () -> {
                        long RecievedTime = System.currentTimeMillis(); 

                        //convert grab the message contents 
                        byte[] content= n.getMessageContents();

                        Map<Long, Long> gossip = deserializeHashMap(content); 
                        verboseLog.fine("Sender is " + n.getSenderPort() + ". Recieved at " + RecievedTime+"\nContents of acquired message :\n" + gossip.toString());
                        for(HashMap.Entry<Long, Long> entry: myGossipMap.entrySet()){
                            long gossipEDserverId = entry.getKey(); 

                            Long myHeartBeat = entry.getValue();
                            Long otherHeartBeat = gossip.get(gossipEDserverId);  
                            boolean deleted = (otherHeartBeat == null); 

                            if(deleted || myHeartBeat >= otherHeartBeat){ //need to ask about equal sign
                                if(RecievedTime - this.TimeLastUpdatedMap.get(gossipEDserverId) > Fail){ //might want to reverse this and take away Math
                                    //no longer in my gossip 
                                    this.myGossipMap.remove(gossipEDserverId); 
                                    //should also remove from TimeLastUdpateMap
                                    //for broadcasting 
                                    InetSocketAddress address =this.peerMap.remove(gossipEDserverId); 
                                    this.verboseLog.fine("new peer map == " +this.peerMap); 
                                    //for UDP to ignore these messages
                                    this.server.deadServers.add(address); 
                                    //I need to synchronize ports with the thread In Gossip Tracker that is sending the messages
                                    //every 3 seconds
                                    synchronized(ports){
                                        this.ports = new ArrayList<>();
                                        for(InetSocketAddress socket: this.peerMap.values()){
                                            this.ports.add(socket); 
                                        }
                                    }
                                    
                                    this.shortLog.fine(myId +": no heartbeat from server " + gossipEDserverId + " - SERVER FAILED");
                                    System.out.println(myId +": no heartbeat from server " + gossipEDserverId + " - SERVER FAILED");

                                    //check if this removed noe was the leader
                                    Vote v = this.server.currentLeader; 
                                    ServerState myState = this.server.getPeerState(); 
                                    if(v.getProposedLeaderID() == gossipEDserverId){

                                        this.server.peerEpoch++;
                                        try {
                                            this.server.setCurrentLeader(null);
                                        } catch (IOException e) {
                                            this.verboseLog.fine("something went wrong with setting the leader to null\n" + e.getMessage()); 
                                        }
                                        if(myState != ServerState.OBSERVER){
                                            //synchronized(server){
                                                this.server.setPeerState(ServerState.LOOKING);
                                            //}
                                            this.shortLog.fine(myId + ": switching from " + myState + " to " + ServerState.LOOKING);
                                            System.out.println(myId + ": switching from " + myState + " to " + ServerState.LOOKING);
                                        }else{
                                            //need to lock before accepting any requests again
                                            synchronized(this.server.leaderLock){
                                                this.verboseLog.fine("Gateway is setting the lock to false"); 
                                                this.server.leaderLock.ready = false; 
                                            }
                                        }
                                    }
                                }
                            }else{
                                this.myGossipMap.replace(gossipEDserverId,  otherHeartBeat); 
                                this.TimeLastUpdatedMap.put(gossipEDserverId, RecievedTime); 
                                this.shortLog.fine(myId +": updated "+gossipEDserverId+"'s heartbeat sequence to "+otherHeartBeat+" based on message from "+ portToId.get(n.getSenderPort())+ " at node time " + RecievedTime + '\n');
                            }
                        }

                    };

                    try{
                        this.cacheExec.submit(task); 
                    }catch(Exception e){}

                }
                else{
                    wait = Math.min(wait * 2, mostWait); 
                }
            } catch (InterruptedException e) {
                this.verboseLog.fine(e.getMessage());
            } 
        }
        this.shortLog.fine("Tracker shutting down");
        this.verboseLog.fine("Tracker shutting down");
    }


    private class VerboseHandler implements HttpHandler{

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String filename ="VerboseGossipTracking-on-port-" + gossipPort + ".log"; 
            Path logfilePath = Paths.get(dir, filename); 

            if(Files.exists(logfilePath)){
                byte[] content = Files.readAllBytes(logfilePath); 
                exchange.sendResponseHeaders(200, content.length); 
                OutputStream os = exchange.getResponseBody(); 
                os.write(content); 
                os.close();
            }
        }
        
    }
    
    private class AbridgedHandler implements HttpHandler{

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String filename ="AbridgedGossipTracking-on-port-" + gossipPort + ".log"; 
            Path logfilePath = Paths.get(dir, filename); 

            if(Files.exists(logfilePath)){
                byte[] content = Files.readAllBytes(logfilePath); 
                exchange.sendResponseHeaders(200, content.length); 
                OutputStream os = exchange.getResponseBody(); 
                os.write(content); 
                os.close();
            }
        }
        
    }

    public static byte[] serializeHashMap(Map<Long, Long> copy) {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream objectStream = new ObjectOutputStream(byteStream);

            // Serialize the HashMap
            objectStream.writeObject(copy);

            // Close streams
            objectStream.close();
            byteStream.close();

            // Get the byte array
            return byteStream.toByteArray();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Map<Long, Long> deserializeHashMap(byte[] serializedData) {
        try {
            ByteArrayInputStream byteStream = new ByteArrayInputStream(serializedData);
            ObjectInputStream objectStream = new ObjectInputStream(byteStream);

            @SuppressWarnings("unchecked")
            Map<Long, Long> wrapper = (Map<Long, Long>) objectStream.readObject();

            objectStream.close();
            byteStream.close();

            return wrapper;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    } 

    
}
