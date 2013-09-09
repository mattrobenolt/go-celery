package celery

type ResultStatus string

const (
	StatusSuccess ResultStatus = "SUCCESS"
)

// {'status': 'SUCCESS', 'traceback': None, 'result': 2, 'task_id': 'd3858e68-48da-4631-b42b-7dbd0ffa08d1', 'children': []}

type Result struct {
	status ResultStatus
	traceback []string
	result interface{}
	task_id string
	children []string
}
