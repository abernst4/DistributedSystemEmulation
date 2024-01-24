package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import edu.yu.cs.com3800.ElectionNotification;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.ZooKeeperLeaderElection;
import edu.yu.cs.com3800.Message.MessageType;

public class RoundRobinLeader extends Thread implements LoggingServer{
    //int index; 
    //int size; 
    Logger logger;  
    int myPort; 
    //List<Integer> ports; 
    LinkedBlockingQueue<InetSocketAddress> ports; 
    Queue<byte[]> myWorkerCache; 
    ServerSocket socket; 
    boolean removed; 
    ExecutorService executor; 
    int gatewayPort; 
    boolean shutdown; 
    ZooKeeperPeerServerImpl server; 
    Map<Long, Message> cache; 
    Set<InetSocketAddress> deadServers; 
    public RoundRobinLeader(int myPort, LinkedBlockingQueue<InetSocketAddress> ports, int gatewayPort, ZooKeeperPeerServerImpl server, Queue<byte[]> myWorkerCache) throws IOException{
        this.myPort = myPort + 2; 
        this.gatewayPort = gatewayPort; 
        this.deadServers = server.deadServers; 
        this.myWorkerCache = myWorkerCache; 
        this.ports = ports; 
        this.server = server; 
        this.cache = new ConcurrentHashMap<>(); 
        setDaemon(true); 
        this.socket = new ServerSocket(this.myPort); 
        this.logger = initializeLogging(RoundRobinLeader.class.getSimpleName() + "-on-port-"+ this.myPort); 
        //  this.size = ports.size(); 
        executor = Executors.newCachedThreadPool(); 
    }

    public void shutdown(){
        try {
            this.socket.close();
            this.shutdown = true; 
        } catch (Exception e) {
            this.logger.fine("trying to shutdown Robind"); 
        } 
    }
    @Override
    public void run(){
        //FIRST SEE IF WE HAVE ANYTHING FROM WHEN WE WERE A WORKER - if we were a worker
        Runnable task = ()->{
            if(this.myWorkerCache != null){
                while(!this.myWorkerCache.isEmpty()){
                    Message n = new Message(this.myWorkerCache.poll()); 
                    this.cache.put(n.getRequestID(), n); 
                }
            }
        }; 
        executor.submit(task); 

        //SECOND SEE IF OUR FOLLOWERS have any work for us to do 
        int size = this.ports.size(); 
        CountDownLatch latch = new CountDownLatch(size); 
        for(int i = 0; i < size; i++){
            InetSocketAddress worker = this.ports.poll(); 
            int port = worker.getPort(); 
            this.ports.offer(worker); 
            //make the connectoin
            task = () -> {
                while(true){
                    try{
                        Socket workerSocket = new Socket("127.0.0.1", port);

                        ObjectOutputStream toWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                        Message n = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK, new byte[0], "127.0.0.1", myPort,
                                    "127.0.0.1", port); 
                        byte[] message = n.getNetworkPayload(); 
                        toWorker.writeObject(message);        

                        ObjectInputStream fromWorker = new ObjectInputStream(workerSocket.getInputStream());
                        message = (byte[]) fromWorker.readObject(); 
                        n = new Message(message); 
                        message = n.getMessageContents(); 

                        ByteArrayInputStream byteStream = new ByteArrayInputStream(message); 
                        ObjectInputStream objectStream = new ObjectInputStream(byteStream); 
                        @SuppressWarnings("unchecked")
                        Queue<byte[]> temp = (ArrayDeque<byte[]>) objectStream.readObject(); 
                        while(!temp.isEmpty()){
                            n = new Message(temp.remove()); 
                            this.cache.put(n.getRequestID(), n); 
                        }

                        byteStream.close();
                        objectStream.close();
                        toWorker.close();
                        fromWorker.close(); 
                        workerSocket.close();
                        logger.fine("We have recieved CACHED contents from the port " + port);

                        latch.countDown(); 
                        break;
                    }catch(IOException | ClassNotFoundException e){
                        this.logger.fine(e.getMessage()+"\ntrying again to connect"); 
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e1) {}
                    }
                }
            }; 
            executor.submit(task); 
        }

        try {
            latch.await();
        } catch (InterruptedException e) {}
        
        //SEND THE OBSERVER A TCP MESSAGE so that he can release the lock
        task = () -> {
            try{
                Socket workerSocket = new Socket("127.0.0.1", gatewayPort);

                logger.fine("Sending message to Gateway to release the lock"); 
                ObjectOutputStream toWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                Message n = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK, new byte[0], "127.0.0.1", myPort,
                            "127.0.0.1", gatewayPort); 
                byte[] message = n.getNetworkPayload(); 
                toWorker.writeObject(message);        
                this.logger.fine("sent message from leader to worker"); 

                toWorker.close();
                workerSocket.close();

            }catch(IOException e){

            }
        };
        executor.submit(task); 

        //Fourth start accepting the connections 
        while(!shutdown){
            try{
                //Accept socket from gateway
                Socket gatewaySocket = this.socket.accept(); 
                //int worker_index = this.index++ % this.size; 
                //int worker = this.ports.get(worker_index);  

                task = () -> {
                    Message n, m; 
                    InetSocketAddress socket = this.ports.poll(); 
                    //this is soley to see if the server has died or not
                    InetSocketAddress UDPsocket = new InetSocketAddress("127.0.0.1", socket.getPort()-2); 
                        try{
                            //read the message from the gateway
                            ObjectInputStream fromGateway = new ObjectInputStream(gatewaySocket.getInputStream());
                            ObjectOutputStream toGateway = new ObjectOutputStream(gatewaySocket.getOutputStream());
                            
                            byte[] message = (byte[]) fromGateway.readObject(); 
                            n = new Message(message); 
                            logger.fine("accepted message" + n.toString());
                            
                            if(n.getMessageType() == MessageType.ELECTION){
                                ElectionNotification note = this.server.getElectionNotification(); 
                                byte[] content = ZooKeeperLeaderElection.buildMsgContent(note); 
                                n = new Message(MessageType.ELECTION, content, "127.0.0.1", myPort, "127.0.0.1", n.getSenderPort()); 
                                content = n.getNetworkPayload();
                                toGateway.writeObject(content);

                                fromGateway.close(); 
                                toGateway.close(); 
                                return; 
                            }

                            long requestId = n.getRequestID(); 

                            //USE AN IF STATEMENT TO SEE IF IT'S IN THE CACHE; IF IT IS, THEN USE THAT INSTEAD
                            if(cache.containsKey(requestId)){
                                n = cache.remove(requestId); 

                                message = n.getNetworkPayload(); 
                                toGateway.writeObject(message);
                                this.logger.fine("Sent message FROM Leader to gateway: " + requestId); 
                            
                                //close resources
                                fromGateway.close(); 
                                toGateway.close(); 
                                return; 
                            } 
                            
                            //create a new socket with the server port that we should connect with
                            while(true){
                                try{
                                    if(this.deadServers.contains(UDPsocket)){
                                        String msg = "We are changing workers from " + socket.getPort(); 
                                        socket = this.ports.poll(); 
                                        UDPsocket = new InetSocketAddress("127.0.0.1", socket.getPort()-2);  
                                        this.logger.fine(msg + " to " + socket.getPort());
                                    }
                                    int worker = socket.getPort();  
                                    
                                    Socket workerSocket = new Socket("127.0.0.1", worker);

                                    logger.fine("Leader connected with " + worker); 
                                    ObjectOutputStream toWorker = new ObjectOutputStream(workerSocket.getOutputStream());
                                    m = new Message(MessageType.WORK, n.getMessageContents(), "127.0.0.1", myPort,
                                                "127.0.0.1", worker, requestId); 
                                    message = m.getNetworkPayload(); 
                                    toWorker.writeObject(message);        
                                    this.logger.fine("sent message from leader to worker"); 

                                    ObjectInputStream fromWorker = new ObjectInputStream(workerSocket.getInputStream());
                                    message = (byte[]) fromWorker.readObject(); 
                                    m = new Message(message); 
                                    this.logger.fine("read message from worker"); 
                                    toWorker.close(); 
                                    fromWorker.close(); 
                                    workerSocket.close(); 
                                    this.ports.offer(socket);
                                    break;  
                                }catch(Exception e){
                                    try {
                                        Thread.sleep(1200);
                                    } catch (InterruptedException e1) {}
                                }
                            }
                            
                            //write it back to the gateway
                            if(m.getErrorOccurred()){
                                n = new Message(m.getMessageType(), m.getMessageContents(), "127.0.0.1", myPort, 
                                                "127.0.0.1", n.getSenderPort(), requestId, true); 
                            }else{
                                n = new Message(m.getMessageType(), m.getMessageContents(), "127.0.0.1", myPort, 
                                            "127.0.0.1", n.getSenderPort(), requestId); 
                            }

                            message = n.getNetworkPayload(); 
                            toGateway.writeObject(message);
                            this.logger.fine("sent message from leader to gateway"); 
                        
                            //close resources
                            fromGateway.close(); 
                            toGateway.close(); 
                            //break; 
                        }
                        catch(IOException | ClassNotFoundException e){ 
                            this.logger.fine(e.getMessage()); 
                        }
                    
                };

                executor.submit(task); 
            }catch(Exception e){
                Thread.currentThread().interrupt();  
            }
        }
        this.executor.shutdown(); 
        this.logger.fine("Round robin exiting"); 
    }
}