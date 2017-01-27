package plugin

import (
	harvest "github.com/nobu-k/sb-harvest"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
)

func init() {
	bql.RegisterGlobalSinkCreator("soracom_harvest",
		bql.SinkCreatorFunc(harvest.CreateSink))
}
