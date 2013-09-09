package celery

type ResultStatus string

const (
	StatusSuccess ResultStatus = "SUCCESS"
)

// {'status': 'SUCCESS', 'traceback': None, 'result': 2, 'task_id': 'd3858e68-48da-4631-b42b-7dbd0ffa08d1', 'children': []}

type Result struct {
	Status ResultStatus `json:"status"`
	Traceback []string `json:"traceback"`
	Result interface{} `json:"result"`
	Id string `json:"task_id"`
	Children []string `json:"children"`
}
