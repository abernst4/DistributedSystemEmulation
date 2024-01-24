package edu.yu.cs.com3800.stage5;
//THIS WAS A CLASS THAT I USED FOR TESTING  
/* 
package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.Vote;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ZooKeeperQuorumPeerServerDemo {
    public static void main(String[] args) {
        //tradeMessages1(); //test passed 
        tradeMessages2();//test passed
    }

    //there are two tests that need to be run: 
    //where the observer is the 4th IP: this test is correct since all of them have a leader of 3
    public static void tradeMessages1() {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
        //peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
        //peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        //peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
        //peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));
        //peerIDtoAddress.put(9L, new InetSocketAddress("localhost", 8090));

        //create workers
        ArrayList<ZooKeeperPeerServer> servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if(entry.getKey() != 4L){
                HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
                map.remove(entry.getKey());
                ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 4L);
                servers.add(server);
                //new Thread(server, "Server on port " + server.getAddress().getPort()).start();
                server.start(); 
            }
        }
        //create observer
        HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
        map.remove(4L);
        ZooKeeperPeerServerImpl gateway = new GatewayServerImpl(8040, 0, 4L, map);
        servers.add(gateway);
        //new Thread(gateway, "Server on port " + 8040).start();
        gateway.start(); 

        //wait for threads to start
        try {
            Thread.sleep(1000);
        }
        catch (Exception e) {
        }
        //print out the leaders and shutdown
        for (ZooKeeperPeerServer server : servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
            server.shutdown();
        }
    }
    
    //this is the test where the Observer is one of the first 3 servers
    //the leader should always be 4 in this case 
    public static void tradeMessages2() {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
        //peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
        //peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
        //peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
        //peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));
        //peerIDtoAddress.put(9L, new InetSocketAddress("localhost", 8090));

        //create workers
        ArrayList<ZooKeeperPeerServer> servers = new ArrayList<>(3);
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if(entry.getKey() != 1L){
                HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
                map.remove(entry.getKey());
                ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 1L);
                servers.add(server);
                new Thread(server, "Server on port " + server.getAddress().getPort()).start();
            }
        }
        //create observer
        HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
        map.remove(1L);
        ZooKeeperPeerServerImpl gateway = new GatewayServerImpl(8010, 0, 1L, map);
        servers.add(gateway);
        new Thread(gateway, "Server on port " + 8010).start();

        //wait for threads to start
        try {
            Thread.sleep(1000);
        }
        catch (Exception e) {
        }
        //print out the leaders and shutdown
        for (ZooKeeperPeerServer server : servers) {
            Vote leader = server.getCurrentLeader();
            if (leader != null) {
                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
            }
            server.shutdown();
        }
    }
}

*/