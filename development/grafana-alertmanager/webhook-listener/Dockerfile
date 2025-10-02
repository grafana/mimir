FROM golang:1.24.6

ADD go.mod /go/src/webhook/go.mod
ADD main.go /go/src/webhook/main.go

WORKDIR /go/src/webhook

RUN mkdir /tmp/logs
RUN go mod tidy
RUN go build -o /bin main.go

ENV PORT=8080

ENTRYPOINT [ "/bin/main" ]
