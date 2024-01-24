#This was written by Avi Bernstein and Joseph Borodach

#1 Compile and run my Junit test to warm things up
echo Running the Maven Tests ... | tee -a output.log
mvn test
rm -rf logs*

#2 Create Cluster of 7 nodes and 1 gateway
echo Starting up the clusters of nodes ...
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8000 0 3 | tee -a output.log & 
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8010 1 3 | tee -a output.log &
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8020 2 3 | tee -a output.log &
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8030 3 3 | tee -a output.log &
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8040 4 3 | tee -a output.log &
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8050 5 3 | tee -a output.log &
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8060 6 3 | tee -a output.log &
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8070 7 3 | tee -a output.log &

#steps 3 and 4 are taken care of in the Client class
sleep 5
echo "" | tee -a output.log
echo Starting the client to send requests | tee -a output.log
#9000 is the port of the gateway server; it is explicit in the Stage5PeerServerDemo file
#The second parameter is whether I want to wait for the leader to be elected or not
java_code="package edu.yu.cs.com3800.stage5;

    public class HelloWorld
    {
        public String run()
        {
            return \"Hello world from version <request number>!\";
        }
    }"
echo Input is: | tee -a output.log 
echo "$java_code" | tee -a output.log
echo "" | tee -a output.log
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Client 9000 true | tee -a output.log

#5 kill worker and print out the list - the node should not be here after doing the GET 
echo "" | tee -a output.log
echo We are going to kill worker Server 0 | tee -a output.log
#pkill -f java -cp /target/classes/\ edu.yu.cs.com3800.stage5.Stage5PeerServerDemo\ 8000
pkill -f "java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8000 0 3"
sleep 30 
curl -s http://127.0.0.1:9000/checkleader | tee -a output.log

#6 kill the leader, have a new election, send a bunch more requests to the follower 
echo "" | tee -a output.log
echo We are going to kill the leader Server 7 | tee -a output.log
#pkill -f java -cp /target/classes/\ edu.yu.cs.com3800.stage5.Stage5PeerServerDemo\ 8070
pkill -f "java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Stage5PeerServerDemo 8070 7 3"
sleep 1
#6 and #7 the code in Client will wait for the 
echo Sending requests to servers | tee -a output.log
java -cp target/classes/ edu.yu.cs.com3800.stage5.demo.Client 9000 false | tee -a output.log &
#make get request that will stall and get the leader 
#back_ground=$!
#wait "$back_ground"
sleep 37
curl -s http://127.0.0.1:9000/getleader | tee -a output.log

#Send one more response in the foreground and print the response 
java_code="package edu.yu.cs.com3800.stage5;

    public class HelloWorld
    {
        public String run()
        {
            return \"Hello world!\";
        }
    }"
echo "" | tee -a output.log
echo "" | tee -a output.log
echo Making request in the for ground. Input:| tee -a output.log
echo "$java_code"| tee -a output.log
echo "" | tee -a output.log
curl -s -X POST -H "content-type: text/x-java-source" -d "$java_code" http://localhost:9000/compileandrun | tee -a output.log
echo ""| tee -a output.log
echo ""| tee -a output.log
#9 print out all of log files for all of the nodes
echo Printing the log files that are required| tee -a output.log
log_directories=$(find . -type d -name "logs*")
for dir in $log_directories; do
    #find "$dir" -maxdepth 1 -type f | grep -v '\.lck$'
    find "$dir" -maxdepth 1 -type f -exec grep -l "Gossip" {} \; | grep -v '\.lck$' | tee -a output.log
done

#10 shutdown all nodes
pkill -f java






