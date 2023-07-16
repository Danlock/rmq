# rmq
A RabbitMQ library for Go, built on top of amqp091.

streadway/amqp, the library the RabbitMQ authors forked, is a stable, thin client for communicating to RabbitMQ, but lacks many of the features present in RabbitMQ libraries from other languages. Many redialable AMQP connections have been reinvented in Go codebases everywhere.

This package attempts to provide a wrapper of useful features on top of amqp091, in the hopes of preventing at least one more unnecessary reinvention.
