package xmlHandler

import "testing"

func TestToTimestamp(t *testing.T) {
	tests := []struct {
		pubdate  string
		expected int64
	}{
		{"Mon, 16 Sep 2024 16:28:02 GMT", 1726504082},
		{"Tue, 01 Jan 2019 00:00:00 GMT", 1546300800},
		{"Invalid Date", 0},
	}

	for _, test := range tests {
		result := toTimestamp(test.pubdate)
		if result != test.expected {
			t.Errorf("For pubdate %s, expected %d but got %d", test.pubdate, test.expected, result)
		}
	}
}
