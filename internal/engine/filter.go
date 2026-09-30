package engine

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
)

// FilterOp is how a Filter compares a metadata value.
type FilterOp int

const (
	OpEq     FilterOp = iota // value equals Values[0]
	OpNe                     // key missing, or value differs from Values[0]
	OpIn                     // value is one of Values
	OpNotIn                  // key missing, or value is none of Values
	OpGt                     // value, read as a number, > Values[0]
	OpGte                    // >=
	OpLt                     // <
	OpLte                    // <=
	OpExists                 // key is present, any value
)

// Filter is one condition on a vector's metadata. Search keeps vectors that
// match every filter it's given.
type Filter struct {
	Key    string
	Op     FilterOp
	Values []string
}

// Eq is the common case: key must equal value.
func Eq(key, value string) Filter { return Filter{Key: key, Op: OpEq, Values: []string{value}} }

// ErrBadFilter is returned for a filter with the wrong number of values or
// a non-numeric value on a numeric comparison.
var ErrBadFilter = errors.New("engine: bad filter")

func (f Filter) validate() error {
	switch f.Op {
	case OpEq, OpNe:
		if len(f.Values) != 1 {
			return fmt.Errorf("%w: %q needs exactly one value", ErrBadFilter, f.Key)
		}
	case OpIn, OpNotIn:
		if len(f.Values) == 0 {
			return fmt.Errorf("%w: %q needs at least one value", ErrBadFilter, f.Key)
		}
	case OpGt, OpGte, OpLt, OpLte:
		if len(f.Values) != 1 {
			return fmt.Errorf("%w: %q needs exactly one value", ErrBadFilter, f.Key)
		}
		if _, err := strconv.ParseFloat(f.Values[0], 64); err != nil {
			return fmt.Errorf("%w: %q compares against non-number %q", ErrBadFilter, f.Key, f.Values[0])
		}
	case OpExists:
	default:
		return fmt.Errorf("%w: unknown op %d", ErrBadFilter, f.Op)
	}
	return nil
}

func (f Filter) match(md map[string]string) bool {
	v, ok := md[f.Key]
	switch f.Op {
	case OpEq:
		return ok && v == f.Values[0]
	case OpNe:
		return !ok || v != f.Values[0]
	case OpIn:
		return ok && slices.Contains(f.Values, v)
	case OpNotIn:
		return !ok || !slices.Contains(f.Values, v)
	case OpExists:
		return ok
	}
	// Numeric comparison. A value that isn't a number never matches.
	x, err := strconv.ParseFloat(v, 64)
	if !ok || err != nil {
		return false
	}
	want, _ := strconv.ParseFloat(f.Values[0], 64)
	switch f.Op {
	case OpGt:
		return x > want
	case OpGte:
		return x >= want
	case OpLt:
		return x < want
	default:
		return x <= want
	}
}

func matchesFilters(md map[string]string, filters []Filter) bool {
	for _, f := range filters {
		if !f.match(md) {
			return false
		}
	}
	return true
}
