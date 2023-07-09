package util

import (
	"fmt"
	"strings"
	"testing"
)

var decodeCamelCaseTests = []struct {
	in  string
	out string
}{
	{"", ""},
	{"c", "c"},
	{"C", "c"},
	{"cc", "cc"},
	{"Cc", "cc"},
	{"CC", "cc"},
	{"cC", "c_c"},
	{"ccc", "ccc"},
	{"Ccc", "ccc"},
	{"CCc", "c_cc"},
	{"CCC", "ccc"},
	{"Ccase", "ccase"},
	{"CCase", "c_case"},
	{"cCase", "c_case"},
	{"CamelC", "camel_c"},
	{"CamelCase", "camel_case"},
	{"camelCase", "camel_case"},
	{"Camelcase", "camelcase"},
	{"CAMELCase", "camel_case"},
	{"CamelCASE", "camel_case"},
	{"CAMELCASE", "camelcase"},
	{"camelsRUs", "camels_r_us"},
}

func TestDecodeCamelCase(t *testing.T) {
	for _, tt := range decodeCamelCaseTests {
		t.Run(tt.in, func(t *testing.T) {
			s, err := DecodeCamelCase(tt.in)
			if s != tt.out {
				t.Errorf("got %q, %v; want %q, %v",
					s, err, tt.out, error(nil))
			}
		})
	}
}

func BenchmarkDecodeCamelCase(b *testing.B) {
	for x := 0; x < b.N; x++ {
		for _, tt := range decodeCamelCaseTests {
			_, _ = DecodeCamelCase(tt.in)
		}
	}
}

//func TestDecodeCamelCaseAlt(t *testing.Table) {
//        for _, tt := range decodeCamelCaseTests {
//                t.Run(tt.in, func(t *testing.Table) {
//                        s, err := decodeCamelCaseAlt(tt.in)
//                        if s != tt.out {
//                                t.Errorf("got %q, %v; want %q, %v",
//                                        s, err, tt.out, error(nil))
//                        }
//                })
//        }
//}

//func BenchmarkDecodeCamelCaseAlt(b *testing.B) {
//        for x := 0; x < b.N; x++ {
//                for _, tt := range decodeCamelCaseTests {
//                        _, _ = decodeCamelCaseAlt(tt.in)
//                }
//        }
//}

var decodeCamelCaseTripleTests = []struct {
	in1 rune
	in2 rune
	in3 rune
	out string
}{
	{0, 0, 0, ""},
	{0, 0, 'a', ""},
	{0, 0, 'A', ""},
	{0, 'a', 0, "a"},
	{0, 'A', 0, "a"},
	{'a', 0, 0, ""},
	{'A', 0, 0, ""},
	{0, 'a', 'b', "a"},
	{0, 'a', 'B', "a"},
	{0, 'A', 'b', "a"},
	{0, 'A', 'B', "a"},
	{'a', 'b', 0, "b"},
	{'a', 'B', 0, "_b"},
	{'A', 'b', 0, "b"},
	{'A', 'B', 0, "b"},
	{'a', 0, 'b', ""},
	{'a', 0, 'B', ""},
	{'A', 0, 'b', ""},
	{'A', 0, 'B', ""},
	{'a', 'b', 'c', "b"},
	{'a', 'b', 'C', "b"},
	{'a', 'B', 'c', "_b"},
	{'A', 'b', 'c', "b"},
	{'a', 'B', 'C', "_b"},
	{'A', 'B', 'c', "_b"},
	{'A', 'b', 'C', "b"},
	{'A', 'B', 'C', "b"},
}

func TestDecodeCamelCaseTriple(t *testing.T) {
	var b strings.Builder
	for _, tt := range decodeCamelCaseTripleTests {
		in := fmt.Sprintf("(%q, %q, %q)", tt.in1, tt.in2, tt.in3)
		t.Run(in, func(t *testing.T) {
			b.Reset()
			err := decodeCamelCaseTriple(tt.in1, tt.in2, tt.in3,
				&b)
			if b.String() != tt.out {
				t.Errorf("got %q, %v; want %q, %v",
					b.String(), err, tt.out, error(nil))
			}
		})
	}
}
