package contentpubsub

import "time"

type SetupPubSub struct {

	// Maximum number of subscribers the pubisher can have in a geographical region
	MaxSubsPerRegion int

	// # nodes of a region that are not delegated, to keep the most powerfull nodes recruitable for the future
	PowerSubsPoolSize int

	// Time resend operation if not acknowledge
	OpResendRate time.Duration

	// Number of backups, localized tolerable faults
	FaultToleranceFactor int

	// How many parallel operations of each type can be supported
	ConcurrentProcessingFactor int

	// Maximum allowed number of attributes per predicate
	MaxAttributesPerPredicate int

	// Frequency in which a subscriber needs to resub
	SubRefreshRateMin time.Duration

	// Time the publisher waits for rv ack until it resends event
	TimeToCheckDelivery time.Duration

	// Geographic region of the peer
	Region string

	// Number of peer he may help in FastDelivery
	Capacity int

	// True to activate redirect mechanism
	RedirectMechanism bool

	// True to activate the tracking mechanism and operation acknowledgement
	ReliableMechanisms bool

	// Should be true if we are running with our testground testbed
	TestgroundReady bool

	// timeout of each rpc
	RPCTimeout time.Duration
}

func DefaultConfig(region string, cap int) *SetupPubSub {

	cfg := &SetupPubSub{
		MaxSubsPerRegion:           5,
		PowerSubsPoolSize:          2,
		OpResendRate:               10 * time.Second,
		FaultToleranceFactor:       2,
		ConcurrentProcessingFactor: 50,
		MaxAttributesPerPredicate:  5,
		SubRefreshRateMin:          15 * time.Minute,
		TimeToCheckDelivery:        30 * time.Second,
		RPCTimeout:                 10 * time.Second,
		Region:                     region,
		Capacity:                   cap,
		RedirectMechanism:          true,
		ReliableMechanisms:         true,
		TestgroundReady:            false,
	}

	return cfg
}
