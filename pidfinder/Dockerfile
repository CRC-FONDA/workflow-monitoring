FROM golang:1.19 as build
WORKDIR /go/src/github.com/srnbckr/pidfinder/
ENV CGO_ENABLED=0
COPY . ./
RUN go build -o ./build/pidfinder .

FROM alpine:latest
COPY --from=build /go/src/github.com/srnbckr/pidfinder/build/pidfinder .

ENTRYPOINT [ "./pidfinder" ]