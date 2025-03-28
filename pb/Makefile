default: all

.PHONY: all
all: genproto genfbs

.PHONY: genproto
genproto:
	protoc filer.proto --go_out=./filer_pb --go-grpc_out=./filer_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc iam.proto --go_out=./iam_pb --go-grpc_out=./iam_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc master.proto --go_out=./master_pb --go-grpc_out=./master_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc remote.proto --go_out=./remote_pb --go-grpc_out=./remote_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative
	protoc volume_server.proto --go_out=./volume_server_pb --go-grpc_out=./volume_server_pb --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative

.PHONY: genfbs
genfbs:
	flatc --go -o . --go-namespace message_fbs message.fbs