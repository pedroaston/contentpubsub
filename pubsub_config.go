package contentpubsub

import "time"

type SetupPubSub struct {

	// Maximum number of subscribers the pubisher can have in a geographical region
	MaxSubsPerRegion int

	// # nodes of a region that are not delegated, to keep the most powerfull nodes recruitable for the future
	PowerSubsPoolSize int

	// Number of backups, localized tolerable faults
	FaultToleranceFactor int

	// How many parallel operations of each type can be supported
	ConcurrentProcessingFactor int

	// Maximum allowed number of attributes per predicate
	MaxAttributesPerPredicate int

	// Frequency in which a subscriber needs to resub in minutes
	SubRefreshRateMin time.Duration

	// Geographic region of the peer
	Region string

	// Number of peer he may help in FastDelivery
	Capacity int
}

func DefaultConfig(region string, cap int) *SetupPubSub {

	cfg := &SetupPubSub{
		MaxSubsPerRegion:           5,
		PowerSubsPoolSize:          2,
		FaultToleranceFactor:       2,
		ConcurrentProcessingFactor: 25,
		MaxAttributesPerPredicate:  5,
		SubRefreshRateMin:          time.Duration(15),
		Region:                     region,
		Capacity:                   cap,
	}

	return cfg
}
