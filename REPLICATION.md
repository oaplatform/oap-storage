## Replication
Replication allows to sustain the same data between master and slaves.
For instance cluster works with nodes, some of them are master, some defined as slaves. Any transaction should be broad across all nodes, 
so replication mechanism works here. It collects all changes from master and provides short and simple structure to move changes to nodes where data is stale.
Usually there is a gap between updates, depending on settings, and during this gap master nodes are collecting changes and after gap is over 
all those changes are sent to slave nodes.
Note: as there is a gap between replication there is a risk to drop newer data from master which have not been sent yet.

## Replication here is based on MemoryStorage
~~~
var master = new MemoryStorage<>(...)
var slave = new MemoryStorage<>(...);
var replicator = new Replicator<>( slave, master, 50 )
~~~
Replicator class allows to collect all changes in master. 
In order to provide additional processing on changes to slave we have to add listener to slave.
~~~
slave.addDataListener( new Storage.DataListener<>() {
    @Override
    public void changed( List<Storage.DataListener.IdObject<?,?>> added,
                         List<Storage.DataListener.IdObject<?,?>> updated,
                         List<Storage.DataListener.IdObject<?,?>> deleted ) {
        // extra processing upon data on slave
    }
} );
~~~

## Instant replication
In order to send replication changes immediately you can call 'replicateNow' method on Replicator
~~~
replicator.replicateNow();
~~~

## Configuration
Let's have some BeanStorage where Bean classes are stored. 
We have to define master and slave storages to allow replication process.
Also we have to provide interval of time between replications, see 'interval' property
The 'remote.name' property is used to refer to a bean that is hosted on a different server. 
This is crucial to the RPC mechanism as it allows communication between beans in different JVMs.
~~~
bean-storage-slave {
    implementation = io.xenoss.bidder.BidderCampaignStorage
    supervision.supervise = true
}
bean-storage-master {
    implementation = oap.storage.ReplicationMaster
    remote {
      name = bean-storage
      url = <url to slave>
      timeout = 2m
    }
}
bean-replicator {
    implementation = oap.storage.Replicator
    parameters {
      slave = modules.this.bean-storage-slave
      master = modules.this.bean-storage-master
      interval = 1m
    }
    supervision.supervise = true
}
~~~
and class
~~~
@Slf4j
public class BeanStorage extends MemoryStorage<String, Bean> {
    public Bean() {
        super( Identifier.<Bean>forId( b -> b.id ).build(), Lock.CONCURRENT );
    }

    public void addBeanDataListener( DataListener<String, Bean> dataListener ) {
        super.addDataListener( dataListener );
    }
}

~~~