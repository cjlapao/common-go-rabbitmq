package message

type MessageResult struct {
	Error    error
	Requeue  bool
	Multiple bool
}

func (m MessageResult) Success() bool {
	return m.Error == nil
}

func Handled() MessageResult {
	return MessageResult{
		Error:    nil,
		Requeue:  false,
		Multiple: true,
	}
}

func Unhandled(err error) MessageResult {
	return MessageResult{
		Error:    err,
		Requeue:  false,
		Multiple: false,
	}
}

func UnhandledAndRequeue(err error) MessageResult {
	return MessageResult{
		Error:    err,
		Requeue:  true,
		Multiple: false,
	}
}
