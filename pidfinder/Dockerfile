FROM golang:1.25 AS build
WORKDIR /go/src/github.com/srnbckr/pidfinder/
ENV CGO_ENABLED=0
COPY . ./
RUN go build -o ./build/pidfinder .

FROM alpine:latest
COPY --from=build /go/src/github.com/srnbckr/pidfinder/build/pidfinder .

ENTRYPOINT [ "./pidfinder" ]
