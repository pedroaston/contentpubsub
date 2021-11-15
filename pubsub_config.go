package contentpubsub

import "time"

type SetupPubSub struct {

	// Maximum number of subscribers the pubisher can have in a geographical region
	MaxSubsPerRegion int

	// # nodes of a region that are not delegated, to keep the most powerfull nodes recruitable for the future
	PowerSubsPoolSize int

	// Seconds to resend operation if not acknowledge
	OpResendRate time.Duration

	// Number of backups, localized tolerable faults
	FaultToleranceFactor int

	// How many parallel operations of each type can be supported
	ConcurrentProcessingFactor int

	// Maximum allowed number of attributes per predicate
	MaxAttributesPerPredicate int

	// Frequency in which a subscriber needs to resub in minutes
	SubRefreshRateMin time.Duration

	// Seconds that the publisher waits for rv ack until it resends event
	TimeToCheckDelivery time.Duration

	// Geographic region of the peer
	Region string

	// Number of peer he may help in FastDelivery
	Capacity int

	// True to activate redirect mechanism
	RedirectMechanism bool
}

func DefaultConfig(region string, cap int) *SetupPubSub {

	cfg := &SetupPubSub{
		MaxSubsPerRegion:           5,
		PowerSubsPoolSize:          2,
		OpResendRate:               time.Duration(10),
		FaultToleranceFactor:       2,
		ConcurrentProcessingFactor: 25,
		MaxAttributesPerPredicate:  5,
		SubRefreshRateMin:          time.Duration(15),
		TimeToCheckDelivery:        time.Duration(30),
		Region:                     region,
		Capacity:                   cap,
		RedirectMechanism:          true,
	}

	return cfg
}
