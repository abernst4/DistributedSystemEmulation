package edu.yu.cs.com3800.stage5.demo;
//THIS IS THE CLASS I USED TO TEST

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.stage5.GatewayServer;
import edu.yu.cs.com3800.stage5.GatewayPeerServerImpl;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public class Stage5PeerServerDemo {
    private String validClass = "package edu.yu.cs.com3800.stage3;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
    private String InvalidClass = "package edu.yu.cs.com3800.stage3;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        retrn \"Hello world!\";\n    }\n}\n";

    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    //private int[] ports = {8000, 8010, 8020, 8030};
    private int[] ports = {8000, 8010, 8020, 8030, 8040, 8050, 8060, 8070};
    private int myPort = 9999;
    private InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
    private ArrayList<ZooKeeperPeerServer> servers;
    private GatewayPeerServerImpl gatewayImpl; 
    private GatewayServer gatewayServer; 
    private static int gatewayPort = 9456; 

    public Stage5PeerServerDemo() throws Exception {
        //step 1: create servers
        createServers();
        //step1.1: wait for servers to get started
        try {
            Thread.sleep(5000);
        }
        catch (InterruptedException e) {
        }
        printLeaders();

        //step 2: Since we know the gatewayServer, let's send some Http requests to the gateway and expect 
        //some normal responses
         
        // for (int i = 0; i < 1; i++) {
        //     if(i % 2 == 0){
        //         String message = this.validClass.replace("world!", "world! from code version " + i);
        //         Thread thread = new Thread(new HttpRequestRunnable(gatewayPort, message, true));
        //         thread.setDaemon(true);
        //         thread.start();
        //     }else{
        //         String message = this.InvalidClass.replace("world!", "world! from code version " + i);
        //         Thread thread = new Thread(new HttpRequestRunnable(gatewayPort, message, false));
        //         thread.setDaemon(true);
        //         thread.start();
        //     }
        // }
        
        //let's send 4 messages to the server and expect a normal response 

        try {
            Thread.sleep(1000);
        }
        catch (InterruptedException e) {
        }

        //System.out.println("Killing the leader"); 
        System.out.println("Killing the worker"); 
        ZooKeeperPeerServer killedLeader = servers.remove(0); 
        killedLeader.shutdown();
        try {
            Thread.sleep(40000);
        }
        catch (InterruptedException e) {
        }
        //printLeaders();

        // try {
        //     Thread.sleep(10000);
        // }
        // catch (InterruptedException e) {
        // }
        //printLeaders();
        //step 5: stop servers
        //stopServers();
    }

        static class HttpRequestRunnable implements Runnable {
        private final int serverPort;
        private final String message;
        private final boolean validClass;     
        
        public HttpRequestRunnable(int serverPort, String message, boolean validClass) {
            this.serverPort = serverPort;
            this.message = message;
            this.validClass = validClass; 
        }

        @Override
        public void run() {
            try {
                // Construct URI with fixed server port
                URI uri = URI.create("http://localhost:" + serverPort + "/compileandrun");
                // Build HTTP request
                HttpRequest request = HttpRequest.newBuilder()
                        .header("content-type", "text/x-java-source")
                        .uri(uri)
                        .POST(HttpRequest.BodyPublishers.ofString(message))
                        .build();

                // Send HTTP request and get response
                HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMinutes(10))
                .version(HttpClient.Version.HTTP_1_1)
                .build(); 

                // CompletableFuture<HttpResponse<String>> responseFuture =
                // client.sendAsync(request, HttpResponse.BodyHandlers.ofString());

                // // Block and wait for the response
                // HttpResponse<String> response = responseFuture.get();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                // Print response body
                String output = null; 
                if(validClass){
                    output = "Thread " + Thread.currentThread().getId() +
                            " - Response Body: " + response.body() + "\n" + "Thread " + Thread.currentThread().getId() +
                            " - Response is 200 OK: " + (response.statusCode() == 200); 
                }else{
                    output = "Thread " + Thread.currentThread().getId() +
                            " - Response Body: " + response.body() + "\n" + "Thread " + Thread.currentThread().getId() +
                            " - Response is 400 Error: " + (response.statusCode() == 400); 
                }

                // Print whether response is 200 OK
                System.out.println(output);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    private void printLeaders() {
        for (ZooKeeperPeerServer server : this.servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
        }
    }

    private void stopServers() {
        for (ZooKeeperPeerServer server : this.servers) {
            System.out.println(server.getUdpPort() + " is shutting down");
            server.shutdown();
        }
        this.gatewayServer.shutdown();
    }

    private void printResponses() throws Exception {
        String completeResponse = "";
        for (int i = 0; i < this.ports.length; i++) {
            Message msg = this.incomingMessages.take();
            String response = new String(msg.getMessageContents());
            completeResponse += "Response to request " + msg.getRequestID() + ":\n" + response + "\n\n";
        }
        System.out.println(completeResponse);
    }

    private void sendMessage(String code) throws InterruptedException {
        Message msg = new Message(Message.MessageType.WORK, code.getBytes(), this.myAddress.getHostString(), this.myPort, "localhost", gatewayPort);
        this.outgoingMessages.put(msg);
    }

    private void createServers() throws IOException, InterruptedException {
        //We are going to make 0 and 1 be the workers, 2 be the master and 3 be the observer
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
        for (int i = 0; i < this.ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
        }
        //create servers
        this.servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            if(entry.getKey() != 3L){
                ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 3L);
                this.servers.add(server);
                server.start();
            }else{
                gatewayImpl = new GatewayPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map);
                this.servers.add(gatewayImpl); 
                gatewayImpl.start(); 
            }
        }

        this.gatewayServer = new GatewayServer(gatewayPort, gatewayImpl); 
        this.gatewayServer.start(); 
    }

    public static void main(String[] args) throws Exception {
        //new Stage5PeerServerDemo();
        int[] ports = {8000, 8010, 8020, 8030, 8040, 8050, 8060, 8070};
        int GatewayPort = 9000; 
        Map<Long, InetSocketAddress> peerMap = new HashMap<>(); 
        int myPort = Integer.parseInt(args[0]); 
        long myId = Long.parseLong(args[1]); 
        long gatewayId = Long.parseLong(args[2]); 
        for(int i = 0; i < 8; i++){
            peerMap.put((long)i, new InetSocketAddress("127.0.0.1", ports[i])); 
        }
        peerMap.remove(myId); 

        if(myId == gatewayId){
            GatewayPeerServerImpl gatewayNode = new GatewayPeerServerImpl(myPort, 0, myId, peerMap); 
            gatewayNode.start();
            GatewayServer gateway = new GatewayServer(GatewayPort, gatewayNode); 
            gateway.start(); 
        }
        else{
            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(myPort, 0, myId, peerMap, gatewayId); 
            server.start(); 
        }
    }
}

