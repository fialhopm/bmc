// Package BCD decodes bytes containing values in Binary-coded Decimal format to
// their native representation, e.g. 0x10 = 10, 0x99 = 99.
//
// BCD is an inefficient format (100 possible values per byte for valid input),
// but easily readable from the hex representation.
package bcd

// Decode converts a single BCD byte into its native representation. It accepts
// both packed and unpacked input. For valid input, the output can be 0 through
// 99.
func Decode(b byte) uint8 {
	return ((b&0xf0)>>4)*10 + (b & 0x0f)
}
