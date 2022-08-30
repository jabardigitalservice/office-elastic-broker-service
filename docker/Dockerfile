# build stage
FROM golang:alpine AS build-env
WORKDIR /app
RUN apk --no-cache add build-base git gcc
ADD . /app
RUN go mod download && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o main /app

# final stage
FROM alpine
WORKDIR /app
COPY --from=build-env /app/main /app/
ENTRYPOINT ./main