//go:build rabbit

package rmq_test

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/danlock/rmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func TestHanging(t *testing.T) {
	Example_hanging()
}

func panicOnErr(err error) {
	if err != nil {
		panic(err)
	}
}

func Example_hanging() {
	var tcpConn *net.TCPConn
	amqp091Config := amqp.Config{
		// Set dial so we have access to the net.Conn
		// This is the same as amqp091.DefaultDial(time.Second) except we also grab the connection
		Dial: func(network, addr string) (net.Conn, error) {
			conn, err := net.DialTimeout(network, addr, time.Second)
			if err != nil {
				return nil, err
			}
			if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
				return nil, err
			}
			tcpConn = conn.(*net.TCPConn)
			return conn, nil
		},
	}
	// Create an innocent, unsuspecting amqp091 connection
	amqp091Conn, err := amqp.DialConfig(os.Getenv("TEST_AMQP_URI"), amqp091Config)
	panicOnErr(err)
	// Create a channel to ensure the connection's working.
	amqp091Chan, err := amqp091Conn.Channel()
	panicOnErr(err)
	panicOnErr(amqp091Chan.Close())
	// Betray amqp091Conn expectations by dawdling. While this is unnatural API usage, the intention is to emulate a connection hang.
	dawdlingBegins := make(chan struct{}, 1)
	// hangTime is 3 seconds for faster tests, but this could easily be much longer...
	hangTime := 3 * time.Second
	hangConnection := func(tcpConn *net.TCPConn) {
		go func() {
			sysConn, err := tcpConn.SyscallConn()
			panicOnErr(err)
			// sysConn.Write blocks the whole connection until it finishes
			err = sysConn.Write(func(fd uintptr) bool {
				dawdlingBegins <- struct{}{}
				time.Sleep(hangTime)
				return true
			})
			panicOnErr(err)
		}()
		select {
		case <-time.After(time.Second):
			panic("sysConn.Write took too long!")
		case <-dawdlingBegins:
		}
	}
	hangConnection(tcpConn)
	// The unsuspecting amqp091Conn.Channel() dutifully waits for hangTime.
	// Doesn't matter what amqp.DefaultDial(connectionTimeout) was (only 1 second...)
	chanStart := time.Now()
	amqp091Chan, err = amqp091Conn.Channel()
	panicOnErr(err)
	panicOnErr(amqp091Chan.Close())
	panicOnErr(amqp091Conn.Close())
	// test our expectation that amqp091Conn.Channel hung for at least 90% of hangTime, to prevent flaky tests.
	if time.Since(chanStart) < (hangTime - (hangTime / 10)) {
		panic("amqp091Conn.Channel returned faster than expected")
	}
	// The above demonstrates one of the biggest issues with amqp091, since your applications stuck if the connection hangs,
	// and you don't have any options to prevent this.
	ctx := context.Background()
	// danlock/rmq gives you 2 ways to prevent unbound hangs, Args.AMQPTimeout and the context passed into each function call.
	rmqConnCfg := rmq.ConnectArgs{Args: rmq.Args{Log: slog.Log, AMQPTimeout: time.Second}}
	// Create a paranoid AMQP connection
	rmqConn := rmq.ConnectWithAMQPConfig(ctx, rmqConnCfg, os.Getenv("TEST_AMQP_URI"), amqp091Config)
	// Grab a channel to ensure the connection is working
	amqp091Chan, err = rmqConn.Channel(ctx)
	panicOnErr(err)
	panicOnErr(amqp091Chan.Close())
	// a hung connection, just like we've always feared
	hangConnection(tcpConn)
	// However we will simply error long before hangTime.
	chanStart = time.Now()
	_, err = rmqConn.Channel(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		log.Fatalf("rmqConn.Channel returned unexpected error %v", err)
	}
	chanDur := time.Since(chanStart)
	// rmqConn is too paranoid to hang for 90% of hangTime, but double check anyway
	if time.Since(chanStart) > (hangTime - (hangTime / 10)) {
		log.Fatalf("rmqConn.Channel hung for (%s)", chanDur)
	}
}
