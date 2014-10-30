// FeedFactory implements PipelineFactory and creates feeds

package projector

import (
	"github.com/couchbase/indexing/secondary/protobuf"
	c "github.com/couchbase/indexing/secondary/common"
	pc "github.com/Xiaomei-Zhang/goxdcr/common"
	pp "github.com/Xiaomei-Zhang/goxdcr/pipeline"
	pctx "github.com/Xiaomei-Zhang/goxdcr/pipeline_ctx"
)


type feedFactory struct {
}

var feed_factory feedFactory

func(feedFactory *feedFactory) NewPipeline (topic string) (pc.Pipeline, error) {
	genericPipeline := pp.NewGenericPipeline(topic, make(map[string]pc.Nozzle), make(map[string]pc.Nozzle))
	if pipelineContext, err := pctx.New(genericPipeline); err != nil {
		return nil, err
	} else {
 		genericPipeline.SetRuntimeContext(pipelineContext)
		feed := &Feed{
			engines:            make(map[uint64]*Engine),
			failoverTimestamps: make(map[string]*protobuf.TsVbuuid),
			kvTimestamps:       make(map[string]*protobuf.TsVbuuid),
			reqch:              make(chan []interface{}, c.GenserverChannelSize),
			finch:              make(chan bool),
		}
		feed.GenericPipeline = genericPipeline
		feed.stats = feed.newStats()
		return feed, nil
	}
}

