package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// BufferPoolUsage tracks usage of GetBuffer/PutBuffer calls.
type BufferPoolUsage struct {
	FilePath     string
	LineNumber   int
	FunctionName string
	CallType     string // "GetBuffer" or "PutBuffer"
}

func main() {
	root := "./internal"
	if len(os.Args) > 1 {
		root = os.Args[1]
	}

	getBufferCalls := []BufferPoolUsage{}
	putBufferCalls := []BufferPoolUsage{}

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return nil // Skip files with parse errors
		}

		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					pos := fset.Position(call.Pos())
					funcName := getCurrentFunction(file, call.Pos())

					if sel.Sel.Name == "GetBuffer" {
						getBufferCalls = append(getBufferCalls, BufferPoolUsage{
							FilePath:     path,
							LineNumber:   pos.Line,
							FunctionName: funcName,
							CallType:     "GetBuffer",
						})
					} else if sel.Sel.Name == "PutBuffer" {
						putBufferCalls = append(putBufferCalls, BufferPoolUsage{
							FilePath:     path,
							LineNumber:   pos.Line,
							FunctionName: funcName,
							CallType:     "PutBuffer",
						})
					}
				}
			}
			return true
		})

		return nil
	})

	if err != nil {
		log.Fatal(err)
	}

	// Print summary
	fmt.Println("# Buffer Pool Usage Audit")
	fmt.Println()
	fmt.Printf("## Summary\n")
	fmt.Printf("- Total GetBuffer calls: %d\n", len(getBufferCalls))
	fmt.Printf("- Total PutBuffer calls: %d\n", len(putBufferCalls))
	fmt.Printf("- Balance: %d (should be ~0)\n", len(getBufferCalls)-len(putBufferCalls))
	fmt.Println()

	// Group by domain
	domainStats := make(map[string]struct {
		get int
		put int
	})

	for _, call := range getBufferCalls {
		domain := extractDomain(call.FilePath)
		stats := domainStats[domain]
		stats.get++
		domainStats[domain] = stats
	}

	for _, call := range putBufferCalls {
		domain := extractDomain(call.FilePath)
		stats := domainStats[domain]
		stats.put++
		domainStats[domain] = stats
	}

	fmt.Println("## By Domain")
	fmt.Println("| Domain | GetBuffer | PutBuffer | Balance | Hit% |")
	fmt.Println("|--------|-----------|-----------|---------|------|")

	for domain, stats := range domainStats {
		hitRate := 100.0
		if stats.get > 0 {
			hitRate = float64(stats.put) / float64(stats.get) * 100
		}
		balance := stats.get - stats.put
		fmt.Printf("| %-15s | %9d | %9d | %+7d | %5.1f%% |\n",
			domain, stats.get, stats.put, balance, hitRate)
	}
	fmt.Println()

	// Print detailed listings
	if len(getBufferCalls) < 50 { // Only print details if manageable
		fmt.Println("## GetBuffer Calls")
		for _, call := range getBufferCalls {
			fmt.Printf("- %s:%d - %s\n", call.FilePath, call.LineNumber, call.FunctionName)
		}
		fmt.Println()

		fmt.Println("## PutBuffer Calls")
		for _, call := range putBufferCalls {
			fmt.Printf("- %s:%d - %s\n", call.FilePath, call.LineNumber, call.FunctionName)
		}
	}
}

func extractDomain(path string) string {
	parts := strings.Split(filepath.ToSlash(path), "/")
	for i, part := range parts {
		if part == "domains" && i+1 < len(parts) {
			return parts[i+1]
		}
		if part == "core" && i+1 < len(parts) {
			return "core/" + parts[i+1]
		}
		if part == "protocol" {
			return "protocol"
		}
	}
	return "other"
}

func getCurrentFunction(file *ast.File, pos token.Pos) string {
	var funcName string
	ast.Inspect(file, func(n ast.Node) bool {
		if fn, ok := n.(*ast.FuncDecl); ok {
			if fn.Pos() <= pos && pos <= fn.End() {
				funcName = fn.Name.Name
				return false
			}
		}
		return true
	})
	if funcName == "" {
		return "<unknown>"
	}
	return funcName
}
