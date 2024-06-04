# DistributedSystemEmulation
This project was built over the course of a semester, in which I was learning various Distributed Systems topics. Here are a few that have been implemented: 

## Leader Election and Master-Worker
The zookeeper leader election algorithm has been implemented to determine which server will be the leader; the servers communicate via UDP during the election phase. 
All other servers are worker-nodes; the leader assigns them work based on a Round-Robin algorithm, via TCP. 

## Gossip protocol for failuer detection
The gossip protocol is where every server has a counter, which is the server's 'heartbeat.' Each server keeps track of every other servers' hearbeat. If after X amount of time server 3 has not updated it's 
heartbeat, we consider that server to be down and we remove it from our record of servers in the cluster. Every server has a daemon thread that:

1) Increments a counter, which is the server's heartbeat 
2) Updates it's hearbeat map to see a server has died - and acts accordingly
3) Randomly chooses a node to send this hearbeat

## Load Balancer
The load balancer sits between the client and the master; it's purpose in this project is to send responses to the client from the master and resend client 
requests to the new master - in the event that the previous master goes down. 

