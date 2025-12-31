### todo implement logging mechanism using condvar

## to do finish handle connection.. peers


### process of a node joining a cluster:
Node says hello to supervisor --> NodeToServer::Hello
Supervisor says hello back --> ServerToNode::Welcome
supervisor sends the list of all nodes that he has (and in the network of the new joining node) 
--> ServerToNode::PeerList 
supervisor notify exisiting nodes --> ServerToNode::PeerJoin


new node update his list and sends and expects connection from the nodes //later feature.. check if unique connection: temp_registry, local registry

exisiting nodes if existing.. update their temp registry then wait for the connection 
if arrives then accept and send back