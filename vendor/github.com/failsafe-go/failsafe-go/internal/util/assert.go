package util

func Assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}
