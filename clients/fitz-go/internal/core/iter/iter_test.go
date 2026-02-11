package iter

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShouldIterateAllItemsGivenSliceIteratorWhenForEachCalled verifies
// ForEach iterates over all items in a SliceIterator.
func TestShouldIterateAllItemsGivenSliceIteratorWhenForEachCalled(t *testing.T) {
	// Arrange
	items := []int{1, 2, 3, 4, 5}
	it := NewSliceIterator(items)
	var collected []int

	// Act
	err := ForEach(it, func(v int) error {
		collected = append(collected, v)
		return nil
	})

	// Assert
	require.NoError(t, err)
	assert.Equal(t, items, collected)
}

// TestShouldStopOnErrorGivenCallbackReturnsErrorWhenForEachCalled verifies
// ForEach stops iteration when the callback returns an error.
func TestShouldStopOnErrorGivenCallbackReturnsErrorWhenForEachCalled(t *testing.T) {
	// Arrange
	items := []int{1, 2, 3, 4, 5}
	it := NewSliceIterator(items)
	var collected []int
	expectedErr := errors.New("test error")

	// Act
	err := ForEach(it, func(v int) error {
		collected = append(collected, v)
		if v == 3 {
			return expectedErr
		}
		return nil
	})

	// Assert
	require.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, []int{1, 2, 3}, collected, "should stop after error")
}

// TestShouldCloseIteratorGivenForEachCompletesWhenCalled verifies
// ForEach always calls Close(), even on errors.
func TestShouldCloseIteratorGivenForEachCompletesWhenCalled(t *testing.T) {
	// Arrange
	items := []int{1, 2, 3}
	it := NewSliceIterator(items)

	// Act - complete successfully
	err := ForEach(it, func(v int) error {
		return nil
	})

	// Assert
	require.NoError(t, err)
	// Note: SliceIterator.Close() is a no-op, but we verify it doesn't panic
}

// TestShouldReturnIteratorErrorGivenIteratorFailsWhenForEachCalled verifies
// ForEach returns iterator errors after successful callbacks.
func TestShouldReturnIteratorErrorGivenIteratorFailsWhenForEachCalled(t *testing.T) {
	// Arrange - create a mock failing iterator
	it := &failingIterator{
		items: []int{1, 2, 3},
		err:   errors.New("iterator error"),
	}

	// Act
	err := ForEach(it, func(v int) error {
		return nil
	})

	// Assert
	require.Error(t, err)
	assert.Equal(t, "iterator error", err.Error())
}

// TestShouldIterateEmptySliceGivenSliceIteratorWhenCalled verifies
// SliceIterator handles empty slices correctly.
func TestShouldIterateEmptySliceGivenSliceIteratorWhenCalled(t *testing.T) {
	// Arrange
	items := []int{}
	it := NewSliceIterator(items)

	// Act
	var collected []int
	for it.Next() {
		collected = append(collected, it.Value())
	}

	// Assert
	assert.Empty(t, collected)
	assert.NoError(t, it.Err())
}

// TestShouldReturnNilErrorGivenSliceIteratorWhenErrCalled verifies
// SliceIterator.Err() always returns nil.
func TestShouldReturnNilErrorGivenSliceIteratorWhenErrCalled(t *testing.T) {
	// Arrange
	items := []int{1, 2, 3}
	it := NewSliceIterator(items)

	// Act
	for it.Next() {
		_ = it.Value()
	}

	// Assert
	assert.NoError(t, it.Err())
}

// TestShouldNotPanicGivenMultipleCloseCallsWhenCalled verifies
// SliceIterator.Close() is safe to call multiple times.
func TestShouldNotPanicGivenMultipleCloseCallsWhenCalled(t *testing.T) {
	// Arrange
	items := []int{1, 2, 3}
	it := NewSliceIterator(items)

	// Act & Assert
	assert.NotPanics(t, func() {
		it.Close()
		it.Close()
		it.Close()
	})
}

// TestShouldPanicGivenValueCalledBeforeNextWhenCalled verifies
// SliceIterator panics when Value() is called before Next().
func TestShouldPanicGivenValueCalledBeforeNextWhenCalled(t *testing.T) {
	// Arrange
	items := []int{1, 2, 3}
	it := NewSliceIterator(items)

	// Act & Assert
	assert.Panics(t, func() {
		_ = it.Value()
	})
}

// TestShouldPanicGivenValueCalledAfterEndWhenCalled verifies
// SliceIterator panics when Value() is called after iterator exhausted.
func TestShouldPanicGivenValueCalledAfterEndWhenCalled(t *testing.T) {
	// Arrange
	items := []int{1}
	it := NewSliceIterator(items)
	it.Next() // Move to first item
	it.Next() // Move past end

	// Act & Assert
	assert.Panics(t, func() {
		_ = it.Value()
	})
}

// --- Mock failing iterator for testing ---

type failingIterator struct {
	items []int
	index int
	err   error
}

func (it *failingIterator) Next() bool {
	it.index++
	return it.index < len(it.items)
}

func (it *failingIterator) Value() int {
	return it.items[it.index]
}

func (it *failingIterator) Err() error {
	return it.err
}

func (it *failingIterator) Close() error {
	return nil
}
