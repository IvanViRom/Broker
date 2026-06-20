# Сборка
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Копируем go.mod
COPY go.mod* ./
RUN go mod download || true

# Копируем исходный код
COPY . .

# Собираем все три бинарника
RUN go build -o /broker ./broker
RUN go build -o /queue-manager ./queue-manager
RUN go build -o /client ./client

# Минимальный runtime образ
FROM alpine:latest

WORKDIR /app

# Копируем бинарники из builder
COPY --from=builder /broker /app/broker
COPY --from=builder /queue-manager /app/queue-manager
COPY --from=builder /client /app/client

# Устанавливаем wget для healthcheck
RUN apk add --no-cache wget

# Создаем директорию для данных
RUN mkdir -p /data

EXPOSE 8080 8081

CMD ["echo", "Use docker-compose to run services"]