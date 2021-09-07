# ScoutSubs-FastDelivery
For more context about this system check [here](https://github.com/pedroaston/smartpubsub-ipfs).

## PubSub Interface
To use this pubsub middleware, you can use the following functions ...

```go
// Geographical region of the user
region := "PT"
// number of nodes he may support in FastDelivery
capacity := 100 
// Create a pubsub instance using a kad-dht instance
pubsub := NewPubSub(kad-dht, DefaultConfig(region, capacity)) 

// Make a subscription (ScoutSubs)
pubsub.MySubscribe("waves T/height R 1 2/period R 10 12")

// Publish a event (ScoutSubs)
pubsub.MyPublish("Waves are pumping at Ericeira, sets of 2 m with a nice 12 second period",
 "waves T/Ericeira T/height R 2 2/period R 12 12")

// Search and Subscribe to multicastGroups of interest (FastDelivery)
pubsub.MySearchAndPremiumSub("waves T/height R 1 2")
// Subscribe to a known premium publisher (FastDelivery)
publisherAddress := "localhost:8888"
groupPredicate := "waves T/height R 0 5"
pubsub.MyPremiumSubscribe("waves T/height R 1 2", publisherAddress, groupPredicate)

// Create and advertise a multicast group and publish a event on it (FastDelivery)
pubsub.CreateMulticastGroup("portugal T")
pubsub.MyPremiumPublish("portugal T",
 "Portugal is one of the european contries with more hours of sun per year", "portugal T")
```

## Instructions to run and test this codebase
- Before running the tests, switch the flag TestgroundReady to false at the utils.go file
- The tests should be run one by one because of port and peer-ID assigning  
- Most of the tests need to be confirmed by analyzing the output

## Latest versions of each variant prepared for testground
### Active
- v0.0.18 >> Base-Unreliable
- v0.1.22 >> Redirect-Unreliable
- v0.4.17 >> Base-Rv-Reliable
- v0.5.22 >> Redirect-Rv-Reliable and latest FastDelivery

### Abandoned
- v0.2.13 >> Redirect-Reliable
- v0.3.9  >> Base-Reliable

### Auxiliar
- v0.13s  >> Used for Debugging
