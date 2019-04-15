# zk-rsh
A ZooKeeper shell written in Rust, inspired by [zk-shell](https://github.com/rgs1/zk_shell)


## But, but _why_? Isn't zk_shell awesome?
It is. But I wanted something quick and compact without the overhead of requiring python and starting its interpreter. It's nice to be able to contact zookeeper quickly in resource constrained machines.
Also, Rust Evangelism Strikeforce Yadda yadda.

## Supported Commands
* cd
* connect
* create
* disconnect
* exit
* get
* ls
* rm
* set
* stat
* cp 

There's a lot of missing stuff that zk-shell supports, but these few should get one started. I'll implement them once as soon as I find the need.
