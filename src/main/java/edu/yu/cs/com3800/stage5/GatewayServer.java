package edu.yu.cs.com3800.stage5;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream; 
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.sun.net.httpserver.*;

import edu.yu.cs.com3800.ElectionNotification;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperLeaderElection;
import edu.yu.cs.com3800.Message.MessageType; 

public class GatewayServer {
    HttpServer gateway; 
    GatewayPeerServerImpl server;
    protected int port; 
    volatile boolean shutdown; 

    public GatewayServer(int port, GatewayPeerServerImpl server) throws IOException{
        this.port = port; 
        this.gateway = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0); 
        this.server = server; 
    }

    private class GetLeader implements HttpHandler, LoggingServer{

        LeaderLock lock; 
        Logger logger; 
        public GetLeader(GatewayPeerServerImpl server){
            this.lock = server.leaderLock; 
            try{
                this.logger = initializeLogging("GetLeaderLogger"); 
            }catch(Exception e){}
        }

		@Override
		public void handle(HttpExchange exchange) throws IOException {
            String response = "The LEADER is Server "; 
            synchronized(lock){
                while(!lock.ready){
                    try {
                        this.logger.fine("The lock is CLOSED"); 
						lock.wait();
					} catch (InterruptedException e) {
					} 
                }
            }
            this.logger.fine("The lock is OPEN"); 
            long leader = server.getCurrentLeader().getProposedLeaderID(); 
            response += leader; 
            exchange.sendResponseHeaders(200, response.length());
            OutputStream os = exchange.getResponseBody(); 
            os.write(response.getBytes()); 
            os.close(); 
		}
        
    }
    
    private class CheckLeaderHandler implements HttpHandler, LoggingServer{

        Logger logger;   
        LeaderLock lock; 
        public CheckLeaderHandler(GatewayPeerServerImpl gatewayServerImpl){
            this.lock = gatewayServerImpl.leaderLock; 
            try {
                this.logger = initializeLogging(CheckLeaderHandler.class.getSimpleName());
            } catch (IOException e) {} 
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if(!lock.ready){
                String sorry = "The leader is either not elected or still building it's cache"; 
                sorry+= "\nThe lock is ready? " + lock.ready; 
                this.logger.fine(sorry); 
                exchange.sendResponseHeaders(200, sorry.length());
                OutputStream os = exchange.getResponseBody(); 
                os.write(sorry.getBytes());
                os.close();
                return; 
            }

            this.logger.fine("we are sending messages to servers to get their positions"); 
            ExecutorService executor = Executors.newCachedThreadPool(); 
            StringBuffer response = new StringBuffer(); 
            for(Map.Entry<Long, InetSocketAddress> entry: server.peerIDtoAddress.entrySet()){
                Runnable task = () -> {
                    int TCPport = entry.getValue().getPort() + 2; 
                    ElectionNotification note = server.getElectionNotification();
                    byte[] content = ZooKeeperLeaderElection.buildMsgContent(note); 
                    Message n = new Message(MessageType.ELECTION, content, "127.0.0.1", port,  "127.0.0.1", TCPport); 
                    content = n.getNetworkPayload(); 
                    try{

                    Socket socket = new Socket( "127.0.0.1", TCPport); 
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream()); 
                    out.writeObject(content);

                    ObjectInputStream is = new ObjectInputStream(socket.getInputStream()); 
                    content = (byte[]) is.readObject(); 
                    n = new Message(content); 

                    note = ZooKeeperLeaderElection.getNotificationFromMessage(n);

                    String msg = "Server " + note.getSenderID() + " is in the " + note.getState() + " state\n"; 
                    response.append(msg); 
                    out.close();
                    is.close();
                    socket.close();
                    }catch(IOException | ClassNotFoundException e){
                        this.logger.fine("an error occured: " + e.getMessage());
                    }
                }; 
                executor.submit(task); 
            }
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {} 


            String msg = "Server " + server.id + " is in the " + server.getPeerState() + " state\n"; 
            response.append(msg); 

            String str = response.toString();
            exchange.sendResponseHeaders(200, str.length());
            OutputStream os = exchange.getResponseBody();
            os.write(str.getBytes()); 
            os.close();
            return; 
        }
        
    }

    


    private class Handler implements HttpHandler, LoggingServer{

        Logger logger; 
        GatewayPeerServerImpl HandlerServer; 
        LeaderLock lock; 
        volatile AtomicLong requestIDer; 
        public Handler(GatewayPeerServerImpl gatewayServer) throws IOException{
            this.logger = initializeLogging(Handler.class.getSimpleName() + "-on-port-" + port); 
            this.lock = gatewayServer.leaderLock; 
            HandlerServer = gatewayServer; 
            requestIDer = new AtomicLong(0); 
        }
        
        @Override
        public void handle(HttpExchange exchange) throws IOException{
            Headers headers = exchange.getRequestHeaders();             
            OutputStream os = exchange.getResponseBody(); 

            if(!"POST".equals(exchange.getRequestMethod())){
                exchange.sendResponseHeaders(405, 0);
                os.write("".getBytes()); 
                os.close(); 
                return; 
            }
            else if(!headers.containsKey("content-type")){
                exchange.sendResponseHeaders(400, 0); 
                os.write("".getBytes()); 
                os.close(); 
                return; 
            }   
            else if(!headers.getFirst("content-type").equals("text/x-java-source")){
                exchange.sendResponseHeaders(400, 0); 
                os.write("".getBytes()); 
                os.close(); 
                return; 
            }

            long requestId = requestIDer.getAndIncrement(); 
            this.logger.fine(requestId+" made it to sending a request"); 
            byte[] data = exchange.getRequestBody().readAllBytes(); 
            //make while loop here so that if there is a bug, we come back
            //make a wait notify on the lock from the zookeeper lock 
            while(true){
                try{
                    //long requestId = requestIDer.getAndIncrement(); 
                    //this.logger.fine(requestId+" made it to sending a request"); 
                    //byte[] data = exchange.getRequestBody().readAllBytes(); 
                    
                    synchronized(lock){
                        while(!lock.ready){
                            lock.wait();
                        }
                    }                    
                    int leaderPort = HandlerServer.leaderPort;
                    Socket socket = new Socket("127.0.0.1", leaderPort); 
                    Message n = new Message(MessageType.WORK, data, "127.0.0.1", port,
                                                        "127.0.0.1", leaderPort, requestId); 
                    byte[] message = n.getNetworkPayload(); 
                    ObjectOutputStream toLeader = new ObjectOutputStream(socket.getOutputStream());
                    toLeader.writeObject(message); 
                    logger.fine("just sent a message: " + n.toString()); 

                    //let's get the response
                    ObjectInputStream fromLeader = new ObjectInputStream(socket.getInputStream());
                    message  = (byte[]) fromLeader.readObject(); 
                    n = new Message(message); 
                    data = n.getMessageContents(); 
                    logger.fine("just recieved a message: " + n.toString()); 

                    //let's write it back to the client 
                    if(n.getErrorOccurred()) exchange.sendResponseHeaders(400, data.length); 
                    else exchange.sendResponseHeaders(200, data.length); 
                    os.write(data); 

                    //close resources
                    toLeader.close();
                    fromLeader.close(); 
                    socket.close(); 
                    os.close(); //need to break here  
                    break; 
                }
                catch(Exception e)
                {
                    this.logger.fine(e.getMessage()); 
                    this.logger.fine(Util.getStackTrace(e)); 
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e1) {
                        this.logger.fine("something went wrong while we were trying to sleep; the only explanation is that someone wants us to shut down"); 
                        Thread.currentThread().interrupt();
                    }
                }//might need to make this a while loop with the interrupt so that we can do this again
            }
        }
    }

    public void shutdown(){
        this.gateway.stop(0); 
    }

    public void start() throws InterruptedException, IOException{
        //int leaderPort = server.getLeaderPort(); //This method gives me the TCP port of the leader
        this.gateway.createContext("/compileandrun", new Handler(this.server)); //NEED TO LOOK AT THIS
        this.gateway.createContext("/checkleader", new CheckLeaderHandler(this.server)); 
        this.gateway.createContext("/getleader", new GetLeader(this.server)); 
        this.gateway.start(); 
    }
}
