package contentpubsub

type HistoryRecord struct {
	receivedEvents   []string
	operationHistory map[string]int
}

func NewHistoryRecord() *HistoryRecord {
	record := &HistoryRecord{operationHistory: make(map[string]int)}

	return record
}

func (r *HistoryRecord) AddOperationStat(opName string) {

	if _, ok := r.operationHistory[opName]; !ok {
		r.operationHistory[opName] = 1
	} else {
		r.operationHistory[opName]++
	}
}
