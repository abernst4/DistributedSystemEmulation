package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer{
    protected final InetSocketAddress myAddress;
    protected final int myPort;
    protected ServerState state;
    protected volatile boolean shutdown;
    protected LinkedBlockingQueue<Message> outgoingMessages;
    protected LinkedBlockingQueue<Message> zookeeperQ;
    protected LinkedBlockingQueue<Message> gossipQ;
    private GossipTracker gossipTracker; 
    public Set<InetSocketAddress> deadServers; 
    public Long id;
    public long peerEpoch;
    protected volatile Vote currentLeader;
    public Map<Long,InetSocketAddress> peerIDtoAddress;
    protected UDPMessageSender senderWorker;
    protected UDPMessageReceiver receiverWorker;
    protected long gatewayId; 
    protected Logger logger; 
    protected JavaRunnerFollower worker; 
    protected RoundRobinLeader robin; 
    protected int leaderPort; 
    protected LeaderLock leaderLock; 
    private ZooKeeperLeaderElection zookeeper; 
    private Thread observerTCP; 

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress, long gatewayId){
        //code here...
        this.myPort = myPort; 
        this.peerEpoch = peerEpoch; 
        this.id = id; 
        this.peerIDtoAddress = new ConcurrentHashMap<>(peerIDtoAddress); 
        this.deadServers = Collections.newSetFromMap(new ConcurrentHashMap<>()); 
        this.myAddress = new InetSocketAddress("127.0.0.1",myPort); 
        this.state = ServerState.LOOKING; 
        this.outgoingMessages = new LinkedBlockingQueue<>(); 
        this.zookeeperQ = new LinkedBlockingQueue<>(); 
        this.gossipQ = new LinkedBlockingQueue<>(); 
        this.gatewayId = gatewayId; 
        this.leaderLock = new LeaderLock(); 
        try {
            this.zookeeper = new ZooKeeperLeaderElection(this, this.zookeeperQ, this.peerIDtoAddress);
        } catch (IOException e) {} 
    }

    @Override
    public void shutdown(){
        this.shutdown = true;
        this.gossipTracker.shutdown();
        if(this.robin != null) { 
            this.robin.interrupt(); 
            this.robin.shutdown(); 
        }
        if(this.worker != null) {
            this.worker.interrupt();
            this.worker.shutdown();
        }
    }

    @Override
    public void run(){
        try{
            //step 1: create and run thread that sends broadcast messages
            this.senderWorker = new UDPMessageSender(this.outgoingMessages, this.myPort); 
            this.senderWorker.start();
            //step 2: create and run thread that listens for messages sent to this server
            this.receiverWorker = new UDPMessageReceiver(this.zookeeperQ, this.gossipQ, this.myAddress, this.myPort, this); 
            this.receiverWorker.start(); 
            //step 3: main server loop
            this.gossipTracker = new GossipTracker(gossipQ, peerIDtoAddress, this.id, this); 
            this.gossipTracker.start(); 

        }catch(IOException e){
            e.printStackTrace();
        }

        //boolean instantiated = false; 
        //flags to tell if we have a daemon thread already running
        try{
            while (!this.shutdown){
                switch (getPeerState()){
                    case LOOKING:
                        //start leader election, set leader to the election winner
                        //ZooKeeperLeaderElection zookeeper = new ZooKeeperLeaderElection(this, this.zookeeperQ); 
                        zookeeper.lookForLeader(this.peerEpoch); 
                        break;
                    case LEADING: 
                        //if the java class has not been created; instantiate it 
                        //System.out.println("State of LEADING: " + !(instantiated)); 
                        if(this.robin == null){
                            //instantiated = true; 

                            int gatewayPort = 0; 
                            LinkedBlockingQueue<InetSocketAddress> ports = new LinkedBlockingQueue<>(); 
                            for(Map.Entry<Long, InetSocketAddress> entry: this.peerIDtoAddress.entrySet()){
                                if(entry.getKey() != gatewayId){
                                    int port = entry.getValue().getPort() + 2; 
                                    ports.offer(new InetSocketAddress("127.0.0.1", port)); 
                                }else{
                                    gatewayPort = entry.getValue().getPort() + 2; 
                                }
                            }

                            Queue<byte[]> workerCache = null; 
                            if(this.worker != null){
                                workerCache = this.worker.cache; 
                                this.worker.shutdown(); 
                                this.worker = null; 
                            }

                            this.robin = new RoundRobinLeader(this.myPort, ports, gatewayPort, this, workerCache); 
                            this.robin.start();  
                        }
                        break; 
                    case FOLLOWING:
                        //if not instantiated yet, instantiate and run
                        if(this.worker == null){
                            this.worker = new JavaRunnerFollower(this.myPort, this);
                            this.worker.start();
                        }
                        break; 
                    case OBSERVER:
                        if(this.currentLeader == null){
                            if(observerTCP == null){
                                observerTCP = new ObserverTCP(leaderLock, myPort); 
                                observerTCP.start(); 
                            }
                            
                            zookeeper.lookForLeader(this.peerEpoch); 
                            this.leaderPort = this.peerIDtoAddress.get(this.currentLeader.getProposedLeaderID()).getPort() + 2; 
                        }
                        break; 
                }
            }
        }
        catch (Exception e) {
           //code...
        }

    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v; 
        
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader; 
    }

    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(), 
                                    myPort, target.getHostString(), target.getPort()); 
        this.outgoingMessages.offer(msg);     
    }

    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target, long requedId) throws IllegalArgumentException {
        Message msg = new Message(type, messageContents, this.myAddress.getHostString(), 
                        myPort, target.getHostString(), target.getPort(), requedId); 
        this.outgoingMessages.offer(msg);     
    }

    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        for(InetSocketAddress target: this.peerIDtoAddress.values()){
            Message msg = new Message(type, messageContents, myAddress.getHostString(), myPort,
                                target.getHostString(), target.getPort()); 
            this.outgoingMessages.offer(msg); 
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.state; 
    }

    @Override
    public void setPeerState(ServerState newState) {
        this.state = newState; 
    }

    @Override
    public Long getServerId() {
        return this.id; 
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch; 
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress; 
    }

    @Override
    public int getUdpPort() {
        return this.myPort; 
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId); 
    }

    @Override
    public int getQuorumSize() {
        return ((this.peerIDtoAddress.size()) / 2) + 1; 
    }

    public ElectionNotification getElectionNotification(){
        while(this.currentLeader == null){
            try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {}
        }
        return new ElectionNotification(currentLeader.getProposedLeaderID(), state, id, currentLeader.getPeerEpoch()); 
    }

    public boolean isPeerDead(InetSocketAddress address){
        return this.deadServers.contains(address); 
    }

}