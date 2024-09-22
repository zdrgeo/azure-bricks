package dispatcher

type Publisher interface{}
type Subscriber interface{}

type Dispatcher interface {
	Publisher
	Subscriber
}
