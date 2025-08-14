.PHONY: mockgen
mockgen:
	go install go.uber.org/mock/mockgen@latest
	mkdir -p ./mock
	mockgen -package mock . ProducerAPI,ConsumerAPI > mock/mock.go 
