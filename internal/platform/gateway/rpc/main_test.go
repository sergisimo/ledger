package rpc_test

import "flag"

var updateGoldenFiles = flag.Bool(
	"update", false,
	"update golden files of tests within rpc package",
)
