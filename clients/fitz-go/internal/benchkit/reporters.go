package benchkit

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"
)

// ReportJSON writes benchmark results as JSON.
func ReportJSON(filename string, results []BaselineResult) error {
	return SaveBaseline(filename, results)
}

// ReportCSV writes benchmark results as CSV.
func ReportCSV(filename string, results []BaselineResult) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	return WriteCSV(f, results)
}

// WriteCSV writes results to CSV format.
func WriteCSV(w io.Writer, results []BaselineResult) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()

	headers := []string{"Name", "OpCount", "TotalTime", "AllocBytes", "AllocCount", "NSPerOp", "BytesPerOp", "AllocsPerOp", "Timestamp"}
	if err := cw.Write(headers); err != nil {
		return err
	}

	for _, r := range results {
		record := []string{
			r.Name,
			fmt.Sprintf("%d", r.OpCount),
			fmt.Sprintf("%d", r.TotalTime.Nanoseconds()),
			fmt.Sprintf("%d", r.AllocBytes),
			fmt.Sprintf("%d", r.AllocCount),
			fmt.Sprintf("%.2f", r.NSPerOp),
			fmt.Sprintf("%.2f", r.BytesPerOp),
			fmt.Sprintf("%.2f", r.AllocsPerOp),
			r.Timestamp.Format("2006-01-02T15:04:05Z07:00"),
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	return nil
}

// ReportText writes benchmark results as formatted text.
func ReportText(filename string, results []BaselineResult) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	return WriteText(f, results)
}

// WriteText writes formatted text benchmark report.
func WriteText(w io.Writer, results []BaselineResult) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)

	fmt.Fprintln(tw, "Benchmark\tOps\tNs/Op\tAllocs/Op\tBytes/Op\tTimestamp")
	fmt.Fprintln(tw, "---\t---\t---\t---\t---\t---")

	// Sort by name for consistent output
	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})

	for _, r := range results {
		fmt.Fprintf(tw, "%s\t%d\t%.2f\t%.2f\t%.2f\t%s\n",
			r.Name,
			r.OpCount,
			r.NSPerOp,
			r.AllocsPerOp,
			r.BytesPerOp,
			r.Timestamp.Format("2006-01-02 15:04:05"),
		)
	}

	return tw.Flush()
}

// ComparisonReport compares two baselines and outputs a formatted report.
func ComparisonReport(w io.Writer, baselineMap, currentMap map[string]BaselineResult) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)

	fmt.Fprintln(tw, "Benchmark\tBaseline Ns/Op\tCurrent Ns/Op\tChange %\tBaseline Allocs\tCurrent Allocs\tChange %")
	fmt.Fprintln(tw, "---\t---\t---\t---\t---\t---\t---")

	// Collect all benchmark names
	names := make(map[string]bool)
	for name := range baselineMap {
		names[name] = true
	}
	for name := range currentMap {
		names[name] = true
	}

	// Sort for consistent output
	var sortedNames []string
	for name := range names {
		sortedNames = append(sortedNames, name)
	}
	sort.Strings(sortedNames)

	// Print comparison
	for _, name := range sortedNames {
		baseline, baselineOk := baselineMap[name]
		current, currentOk := currentMap[name]

		if !baselineOk {
			fmt.Fprintf(tw, "%s\tN/A\t%.2f\tN/A\tN/A\t%d\tN/A\n", name, current.NSPerOp, current.AllocCount)
			continue
		}
		if !currentOk {
			fmt.Fprintf(tw, "%s\t%.2f\tN/A\tN/A\t%d\tN/A\tN/A\n", name, baseline.NSPerOp, baseline.AllocCount)
			continue
		}

		latencyChange := CompareBaseline(baseline, current)
		allocChange := CompareAllocations(baseline, current)

		fmt.Fprintf(tw, "%s\t%.2f\t%.2f\t%+.1f%%\t%d\t%d\t%+.1f%%\n",
			name,
			baseline.NSPerOp,
			current.NSPerOp,
			latencyChange,
			baseline.AllocCount,
			current.AllocCount,
			allocChange,
		)
	}

	tw.Flush()
}
