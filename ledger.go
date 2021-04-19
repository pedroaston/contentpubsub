package contentpubsub

type HistoryRecord struct {
	receivedEvents   []*EventRecord
	operationHistory map[string]int
}

type EventRecord struct {
	eventSource string
	eventData   string
	protocol    string
}

// NewHistoryRecord
func NewHistoryRecord() *HistoryRecord {
	record := &HistoryRecord{operationHistory: make(map[string]int)}

	return record
}

// AddOperationStat
func (r *HistoryRecord) AddOperationStat(opName string) {

	if _, ok := r.operationHistory[opName]; !ok {
		r.operationHistory[opName] = 1
	} else {
		r.operationHistory[opName]++
	}
}

// SaveReceivedEvent
func (r *HistoryRecord) SaveReceivedEvent(eventData string, eventSource string, protocol string) {

	event := &EventRecord{
		eventSource: eventSource,
		eventData:   eventData,
		protocol:    protocol,
	}

	r.receivedEvents = append(r.receivedEvents, event)
}
