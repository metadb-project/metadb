package util

import (
	"fmt"
	"strings"
	"unicode"
)

// decodeCamelCase parses a camel case string into all-lowercase words
// separated by underscores.  A sequence of uppercase letters is interpreted
// as a word, except that the last uppercase letter of a sequence is
// considered the start of a new word if it is followed by a lowercase letter.
func decodeCamelCase(s string) (string, error) {
	var b strings.Builder
	// c1, c2, and c3 are a sliding window of rune triples.
	var c1, c2, c3 rune
	for _, c := range s {
		c1 = c2
		c2 = c3
		c3 = c
		err := decodeCamelCaseTriple(c1, c2, c3, &b)
		if err != nil {
			return "",
				fmt.Errorf("error decoding camel case: %v",
					err)
		}
	}
	// Decode last character.
	c1 = c2
	c2 = c3
	c3 = 0
	err := decodeCamelCaseTriple(c1, c2, c3, &b)
	if err != nil {
		return "",
			fmt.Errorf("error decoding camel case: %v",
				err)
	}
	return b.String(), nil
}

// decodeCamelCaseTriple examines a sequence of three runes c1, c2, and c3;
// decodes c2; and writes the decoded output to b.
func decodeCamelCaseTriple(c1, c2, c3 rune, b *strings.Builder) error {
	var c1u = unicode.IsUpper(c1)
	var c2u = unicode.IsUpper(c2)
	var c3u = unicode.IsUpper(c3)
	var write rune = 0
	var writeBreak bool = false
	switch {
	// First check triples that include zeros.
	case (c2 == 0):
		return nil
	case (c1 == 0) && (c2 != 0) && (c3 != 0):
		fallthrough
	case (c1 == 0) && (c2 != 0) && (c3 == 0):
		write = unicode.ToLower(c2)
	case (c1 != 0) && (c2 != 0) && (c3 == 0):
		switch {
		case !c1u && c2u:
			writeBreak = true
			fallthrough
		case c1u && c2u:
			write = unicode.ToLower(c2)
		case !c1u && !c2u:
			fallthrough
		case c1u && !c2u:
			write = c2
		}
	// Check triples having no zeros.
	case !c1u && !c2u && !c3u:
		fallthrough
	case c1u && !c2u && !c3u:
		fallthrough
	case !c1u && !c2u && c3u:
		fallthrough
	case c1u && !c2u && c3u:
		write = c2
	case !c1u && c2u && !c3u:
		fallthrough
	case c1u && c2u && !c3u:
		fallthrough
	case !c1u && c2u && c3u:
		writeBreak = true
		fallthrough
	case c1u && c2u && c3u:
		write = unicode.ToLower(c2)
	// All cases should have been checked by this point.
	default:
		return fmt.Errorf("unexpected state (%q, %q, %q)", c1, c2, c3)
	}
	// Write decoded characters.
	if writeBreak {
		_, err := b.WriteRune('_')
		if err != nil {
			return err
		}
	}
	_, err := b.WriteRune(write)
	if err != nil {
		return err
	}
	return nil
}

// Alternative implementation

/*
var decodeCamelCaseAltRegexp = regexp.MustCompile(
	`(^[^A-Z]+)|([A-Z][^A-Z]+)|(?:([A-Z]+)([A-Z][^A-Z]+))|([A-Z]+$)`)
*/

// decodeCamelCaseAlt is a more recent implementation that performs
// the same task as the function decodeCamelCase, except that it is
// much simpler, using regular expressions (also 10X slower).
/*
func decodeCamelCaseAlt(s string) (string, error) {
	m := decodeCamelCaseAltRegexp.FindAllStringSubmatch(s, -1)
	var b strings.Builder
	for _, mx := range m {
		for y := 1; y < len(mx); y++ {
			s := mx[y]
			if s == "" {
				continue
			}
			if b.Len() > 0 {
				s = "_" + s
			}
			_, err := b.WriteString(strings.ToLower(s))
			if err != nil {
				return "", fmt.Errorf(
					"Error decoding camel case: %v", err)
			}
		}
	}
	return b.String(), nil
}
*/
