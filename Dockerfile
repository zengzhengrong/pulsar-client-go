# 多重构建，减少镜像大小
# 构建：使用golang 默认版本
FROM golang:1.19-alpine AS Builder

# 容器环境变量添加，会覆盖默认的变量值
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn,direct
ENV TZ="Asia/Shanghai"
# 设置工作区
WORKDIR /go/release

# 把全部文件添加到/go/release目录
COPY . .

# 编译：把main.go编译成可执行的二进制文件，命名为app
# RUN GOOS=linux CGO_ENABLED=0 go build -tags netgo -o app main.go
RUN GOOS=linux CGO_ENABLED=0 GOARCH=amd64 go build -tags netgo -ldflags="-s -w" -installsuffix cgo -o app main.go
# RUN GOOS=linux CGO_ENABLED=0 go build -tags netgo -ldflags="-s -w" -installsuffix cgo -o app main.go

FROM alpine
LABEL maintainer="zengzhengrong"

ENV TZ="Asia/Shanghai"

# 时区纠正
RUN rm -f /etc/localtime \
    && ln -sv /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && echo "Asia/Shanghai" > /etc/timezone

# 在build阶段复制可执行的go二进制文件app
COPY --from=Builder /go/release/app /pulsar-cli
#COPY --from=Builder /go/release/.env /.env

# 给日志创建目录
RUN mkdir /app/
RUN mkdir /app/logs/

# 启动服务
ENTRYPOINT ["/pulsar-cli"]