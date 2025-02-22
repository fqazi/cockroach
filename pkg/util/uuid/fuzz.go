// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Copyright (c) 2018 Andrei Tudor Călin <mail@acln.ro>
// Use of this source code is governed by a MIT-style
// license that can be found in licenses/MIT-gofrs.txt.

// This code originated in github.com/gofrs/uuid.

//go:build gofuzz

package uuid

// Fuzz implements a simple fuzz test for FromString / UnmarshalText.
//
// To run:
//
//	$ go get github.com/dvyukov/go-fuzz/...
//	$ cd $GOPATH/src/github.com/gofrs/uuid
//	$ go-fuzz-build github.com/gofrs/uuid
//	$ go-fuzz -bin=uuid-fuzz.zip -workdir=./testdata
//
// If you make significant changes to FromString / UnmarshalText and add
// new cases to fromStringTests (in codec_test.go), please run
//
//	$ go test -seed_fuzz_corpus
//
// to seed the corpus with the new interesting inputs, then run the fuzzer.
func Fuzz(data []byte) int {
	_, err := FromString(string(data))
	if err != nil {
		return 0
	}
	return 1
}
