package kafka

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestKafkaID_Serialize(t *testing.T) {
	rid := &kafkaID{
		messageID: 8,
	}

	bin := rid.Serialize()
	assert.NotNil(t, bin)
	assert.NotZero(t, len(bin))

	rid.LedgerID()
	rid.EntryID()
	rid.BatchIdx()
	rid.PartitionIdx()
}

func Test_SerializeKafkaID(t *testing.T) {
	bin := SerializeKafkaID(10)
	assert.NotNil(t, bin)
	assert.NotZero(t, len(bin))
}

func Test_DeserializeKafkaID(t *testing.T) {
	bin := SerializeKafkaID(5)
	id, err := DeserializeKafkaID(bin)
	assert.Nil(t, err)
	assert.Equal(t, id, int64(5))
}
