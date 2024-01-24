package edu.yu.cs.com3800;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState;
import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

public class ZooKeeperLeaderElection implements LoggingServer
{
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;
    private ZooKeeperPeerServerImpl myPeerServer; 
    private LinkedBlockingQueue<Message> incomingMessages; 
    private long proposedLeader; 
    private long proposedEpoch; 
    Map<Long, ElectionNotification> votes; 
    private long myId; 
    private Map<Long, InetSocketAddress> peerMap; 
    //for debuggin
    private Logger logger; 

    public ZooKeeperLeaderElection(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, Map<Long, InetSocketAddress> peerMap) throws IOException
    {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.proposedLeader = server.getServerId(); 
        this.myId = server.getServerId(); 
        this.peerMap = peerMap; 
        this.logger = initializeLogging(ZooKeeperLeaderElection.class.getSimpleName()+ "-on-port-" + server.getUdpPort());
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader(long epoch) throws InterruptedException, IOException
    {
        //HERE IS ANTOHER CHANGE
        this.proposedEpoch = epoch; 
        this.logger.fine("this is the epoch for this election: " + epoch); 
        this.proposedLeader = this.myId; 
        //send initial notifications to other peers to get things started
        ServerState mystate = this.myPeerServer.getPeerState(); 
        if(mystate != ServerState.OBSERVER){
            sendNotifications(ServerState.LOOKING);
        }
        this.logger.fine("sending initial notifications");
        this.logger.fine(this.peerMap.toString()); 
        this.votes = new HashMap<>(); 
        int wait = finalizeWait;
        boolean stop = false; 
        //Loop, exchanging notifications with other servers until we find a leader
        while (this.myPeerServer.getPeerState() == ServerState.LOOKING || !stop){
            //Remove next notification from queue, timing out after 2 times the termination time
            //NEED TO TAKE THIS OUT AFTERWARDS 
            //logger.fine(this.incomingMessages.toString()); 

            Message n = this.incomingMessages.poll(wait, TimeUnit.MILLISECONDS); 
            //if no notifications received..
            if (n == null){
                //..resend notifications to prompt a reply from others..
                int size = this.incomingMessages.size(); 
                this.logger.fine(votes.toString());
                if(mystate == ServerState.OBSERVER){
                    sendNotifications(ServerState.OBSERVER);
                }
                else if(size == 0){
                    sendNotifications(ServerState.LOOKING);
                }
                //.and implement exponential back-off when notifications not received..
                wait = Math.min(wait * 2, maxNotificationInterval); 
            }else{
                //if/when we get a message and it's from a valid server and for a valid server..
                this.logger.fine(votes.toString());
                ElectionNotification note = getNotificationFromMessage(n); 

                if(note.getPeerEpoch() < epoch){
                    this.logger.fine("ignoring this message because it had a lower epoch: \n"+ note.toString()); 
                    continue; 
                }
                //switch on the state of the sender:
                ServerState stateOfReciever = note.getState(); 
                switch(stateOfReciever){ 
                    case LOOKING: //if the sender is also looking
                        //I'll only process the message if I am not an observer
                        //the observer will accept the leader when the actual Election message is sent out
                        if(mystate != ServerState.OBSERVER ){
                            //if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is.
                            long Xleader = note.getProposedLeaderID(); 
                            long Xepoch = note.getPeerEpoch(); 
                            this.logger.fine("Recieved vote = "+note.toString()); 
                            if(supersedesCurrentVote(Xleader, Xepoch)){
                                this.logger.fine("changing my vote from " + this.proposedLeader + " to " + Xleader);
                                this.proposedLeader = Xleader; 
                                this.proposedEpoch = Xepoch; 
                                sendNotifications(ServerState.LOOKING);
                            }
                            //keep track of the votes I received and who I received them from.
                            votes.put(note.getSenderID(), note); 
                            ////if I have enough votes to declare my currently proposed leader as the leader:
                            Vote myVote = getCurrentVote(); 
                            if(haveEnoughVotes(votes, myVote)){ 
                                //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
                                //it's really wait
                                while ((n = this.incomingMessages.poll(500, TimeUnit.MILLISECONDS)) != null){
                                    note = getNotificationFromMessage(n);
                                    Vote temp = (Vote)note; 
                                    if(supersedesCurrentVote(temp.getProposedLeaderID(), temp.getPeerEpoch())){
                                    this.incomingMessages.put(n); 
                                    break; 
                                    }
                                }
                                //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone lese won the election) and exit the election
                                if (n == null){
                                    this.logger.fine("I am declaring a leader because I have enough votes"); 
                                    this.logger.fine(this.votes.toString()); 
                                    ServerState state = this.proposedLeader == this.myPeerServer.getServerId()? ServerState.LEADING : ServerState.FOLLOWING;         
                                    this.myPeerServer.setPeerState(state); 
                                    //this.logger.fine("This is my state after winning the election: " + this.myPeerServer.getPeerState());  
                                    this.myPeerServer.setCurrentLeader(new Vote(this.proposedLeader, this.proposedEpoch));
                                    this.logger.fine("this is my leader: " + this.myPeerServer.getCurrentLeader().toString()); 
                                    //this.myPeerServer.peerEpoch = this.proposedEpoch + 1; //CHANGE HERE 
                                    sendNotifications(state);
                                    this.incomingMessages.clear();
                                    stop = true; 
                                }else{
                                    this.logger.fine("I would have declared " + this.proposedLeader+ " as a leader, but I found higher one on the queue");
                                }
                            }
                        }
                        break; 
                    case FOLLOWING: 
                    case LEADING: //if the sender is following a leader already or thinks it is the leader
                        //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                            //votes.put(note.getSenderID(), note);
                            //if so, accept the election winner.
                            //this.logger.fine("I am declaring a leader because I found someone who is Leading or following"); 
                            this.proposedLeader = note.getProposedLeaderID(); 
                            if(mystate != ServerState.OBSERVER){
                                ServerState state = note.getProposedLeaderID() == this.myPeerServer.getServerId()? ServerState.LEADING : ServerState.FOLLOWING;         
                                this.myPeerServer.setPeerState(state);   
                                sendNotifications(state);
                            }
                            this.logger.fine(n.toString()); 
                            this.logger.fine("declaring " + note.getProposedLeaderID() + " as the winner from " + note.getSenderID()); 
                            this.myPeerServer.setCurrentLeader((Vote)note);
                            this.incomingMessages.clear();
                            stop = true; 
                            break;   
                    case OBSERVER:
                            break; 
                }
            }
        }
        this.logger.fine("Leader election ending"); 
        //probably need to switch this
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    private void sendNotifications(ServerState state) {
        //use server.sendBroadcase
        long myID = this.myPeerServer.getServerId(); 
        ElectionNotification note = new ElectionNotification(this.proposedLeader, state, myID, this.proposedEpoch); 
        byte[] content = buildMsgContent(note); 
        //write the Message object with this server asking to be the leader  
        this.myPeerServer.sendBroadcast(MessageType.ELECTION, content);
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
     protected boolean supersedesCurrentVote(long newId, long newEpoch) {
         return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
     }
    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     * 
     * There is no way to tell whether every server has a vote nebecause we can't get the full map from the 
     * server field. 
     */
    //HERE IS ONE CHANGE
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification > votes, Vote proposal)
    {
       //is the number of votes for the proposal > the size of my peer server’s quorum?
       int counter = this.myPeerServer.getQuorumSize(); 
       this.logger.fine("The quorum size is: " + counter); 
       for(Map.Entry<Long, ElectionNotification> entry: votes.entrySet()){
            long id = entry.getKey(); 
            Vote temp = (Vote)entry.getValue(); 
            if(proposal.equals(temp) && this.peerMap.containsKey(id)) --counter; 
       }
       if(proposal.equals(getCurrentVote())) --counter; 
       boolean hasenough = counter <= 0; 
       return hasenough; 
    }

    public static byte[] buildMsgContent(ElectionNotification notification){
        ByteBuffer contentBuffer = ByteBuffer.allocate(8 + 2 + 8 + 8); 
        contentBuffer.putLong(notification.getProposedLeaderID()); 
        char stateChar = notification.getState().getChar(); 
        contentBuffer.putChar(stateChar); 
        contentBuffer.putLong(notification.getSenderID()); 
        contentBuffer.putLong(notification.getPeerEpoch()); 
        return contentBuffer.array(); 
    }

    public static ElectionNotification getNotificationFromMessage(Message received) {
        ByteBuffer msgBytes = ByteBuffer.wrap(received.getMessageContents());
        long leader = msgBytes.getLong();
        char stateChar = msgBytes.getChar();
        long senderID = msgBytes.getLong();
        long peerEpoch = msgBytes.getLong();
        ZooKeeperPeerServer.ServerState state = ZooKeeperPeerServer.ServerState.getServerState(stateChar); 
        return new ElectionNotification(leader, state, senderID, peerEpoch); 
    }
}