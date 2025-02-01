package empty

import (
	"github.com/zdrgeo/azure-bricks/pubsub"
)

type Empty struct{}

func (message *Empty) Discriminator() pubsub.Discriminator {
	return pubsub.DiscriminatorEmpty
}
