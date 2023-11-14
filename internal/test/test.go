package test

import "testing"

func FailOnError(t testing.TB, err error) {
	if err != nil {
		t.Helper()
		t.Fatalf("%+v", err)
	}
}
