# BFT-RAFT-Consensus
Implemented a prototype of voting-based consensus algorithm for permissioned Blockchain named BFT-RAFT (Byzantine Fault Tolerant RAFT) using PBFT (Practical Byzantine Fault Tolerance)’s Byzantine Fault Tolerant design into RAFT to tolerate both Crash and Byzantine faults in a single consensus protocol while still maintaining RAFT’s understandability and higher scalability.

# To Run this Project

1. Create logs folder in the root directory of the project and create files called s1_log.txt, s2_log.txt, and s3_log.txt to store logs of each server node. In each of those files, paste the following and nothing else: 

```
0 0 set unreachable 
1 0 register s1 10001 
2 0 register s2 10002 
3 0 register s3 10003
```

Why: you have to pre-populate the server logs with information about which other servers to contact about elections, or else they just send out broadcasts to no one and keep voting for themselves. This tells the servers who else they're supposed to be contacting.

2. Open three terminal windows. In each one, go to the root directory of the project. Then, in each of the tree, paste one of the following: 

`python start_server.py s1 10001` 
`python start_server.py s2 10002`
`python start_server.py s3 10003`

Now you need to wait 10-20 seconds for someone to get elected the leader.

3. Open a fourth terminal window and navigate to the root directory of the project. Type `python start_client.py 10001`. When it says to type your message, type `client|@get a`

That command includes the name of the sender (client), a pipe to delimit the pipe from its port (which the client does not have, hence no number between | an @), an @ to delimit the sender from the message, and then the message "get a", which asks the server to get the value at the key a in the keystore. There is no value at the key a at the moment. This is fine.

The server will respond either with "name|port@" (if you guessed the port of the leader correctly) or "received b'name|port@I am not the leader. The last leader I heard from is some_other_server_name."
if it's the latter, shut down the client, and then restart the client pointing at the port of the server indicated in the last message as the leader. Then when you send "client|@get a" you should get back from the server "name|port@" That means you're talking to the leader. 

From there you can issue commands like "client|@set a 1". When you send that, the server will probably close the connection on you because it is very rude. However, it will execute and replicate the command (you can verify this by checking the s1_log.txt, s2_log.txt, and s3_log.txt files, or by sending a command from the client "client|@get a" and confirming that the response includes the 1).

### Example of client command
client|@set account1 500

### To run the tests
+ `pytest tests/test_logManager.py --co`
+ `python -m unittest`
