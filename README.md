# Exercise on raft 

I'm experimenting with raft these days so to have a more hands on experience on distributed systems. This exercise follows the awesome tutorial provided by <https://notes.eatonphil.com/minimal-key-value-store-with-hashicorp-raft.html> i'm literally just copy-pasting it, which in itself is a consumer to hashicorp's raft implementation.


## See it in action

Start with 3 terminals

```shell
 go run main.go --node-id=node1 --http-port=8001 --raft-port=9001
 ```

 ```shell
 go run main.go --node-id=node2 --http-port=8002 --raft-port=9002
 ```

 ```shell
 go run main.go --node-id=node3 --http-port=8003 --raft-port=9003
 ```

So, now, you would have to manually assign followers to leaders, by using /join endpoint. it is not the most straightforward api and one would find it irritating.

## Newer features

- add a service discovery
it bothers me that i'll have to lookup through the stdout to see what an endpoint is the current leader