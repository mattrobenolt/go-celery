package celery

type Message struct {
	ContentType string
	Body []byte
	Receipt Receipt
}
