//go:generate protoc -I $PWD -I $GOPATH/src -I $GOPATH/src/github.com/google/protobuf/src --gogoslick_out=. $PWD/protocol.proto
//go:generate sh -c "pbjs protocol.proto -p $PWD -p $GOPATH/src -p $GOPATH/src/github.com/google/protobuf/src -t commonjs > protocol.js"
package test
