package sliceutils

// Map applies a transformation function to each element of the input slice and returns a new slice
// containing the transformed values in the same order.
//
// Parameters:
//   - input: The input slice of type I.
//   - transform: A transformation function that takes an element of type I and returns an element of type O.
//
// Returns:
//   - []O: A new slice containing the transformed values of type O.
//
// Example Usage:
//
//	input := []int{1, 2, 3, 4, 5}
//	double := func(x int) float64 { return float64(x) * 2 }
//	result := Map(input, double)
//
// result will be []float64{2.0, 4.0, 6.0, 8.0, 10.0}.
func Map[I any, O any](input []I, transform func(I) O) []O {
	output := make([]O, len(input))
	if len(input) < 1 {
		return output
	}
	for i, e := range input {
		output[i] = transform(e)
	}
	return output
}
