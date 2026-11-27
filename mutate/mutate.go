// Package mutate generates mutants of Go source files for mutation testing.
//
// A mutant is one small change to one file: a flipped comparison, a negated
// equality, a swapped logical operator, a removed +1 or -1, an if condition
// forced true or false, or a removed assignment or ++/-- statement. Mutants
// are enumerated in a fixed order with stable ids, and applied by returning
// the mutated file's source, which the caller hands to the Go toolchain with
// -overlay so the original file is never modified (docs/design/P8.1).
package mutate

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Operators, in the order they are tried at each node.
const (
	OpBoundary = "boundary"    // < <-> <=, > <-> >=
	OpNegate   = "negate"      // == <-> !=
	OpLogical  = "logical"     // && <-> ||
	OpOffByOne = "off-by-one"  // x+1 -> x, x-1 -> x
	OpIfTrue   = "if-true"     // if cond -> if true
	OpIfFalse  = "if-false"    // if cond -> if false
	OpRemove   = "remove-stmt" // drop an assignment (=, +=, ...) or ++/-- statement
)

// Mutant identifies one change.
type Mutant struct {
	ID   string `json:"id"` // file:line:col:operator, stable across runs
	File string `json:"file"`
	Line int    `json:"line"`
	Col  int    `json:"col"`
	Op   string `json:"op"`
	Desc string `json:"desc"` // e.g. "lastIndex >= l.lastIndex() -> lastIndex > l.lastIndex()"
	Func string `json:"func"` // enclosing function
	nth  int    // index of this site among the file's sites, for Apply
}

// site is a mutation point found while walking a file.
type site struct {
	pos   token.Pos
	op    string
	fn    string
	apply func() // mutates the AST in place
	undo  func()
	desc  func(*token.FileSet) string
}

// Enumerate returns every mutant of the non-test .go files in dir, ordered by
// file name, then position, then operator.
func Enumerate(dir string) ([]Mutant, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	var out []Mutant
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		ms, err := EnumerateFile(f)
		if err != nil {
			return nil, err
		}
		out = append(out, ms...)
	}
	return out, nil
}

// EnumerateFile returns the mutants of one file.
func EnumerateFile(path string) ([]Mutant, error) {
	fset, _, sites, err := load(path)
	if err != nil {
		return nil, err
	}
	out := make([]Mutant, 0, len(sites))
	for i, s := range sites {
		p := fset.Position(s.pos)
		out = append(out, Mutant{
			ID:   fmt.Sprintf("%s:%d:%d:%s", filepath.ToSlash(path), p.Line, p.Column, s.op),
			File: path, Line: p.Line, Col: p.Column, Op: s.op, Func: s.fn,
			Desc: s.desc(fset), nth: i,
		})
	}
	return out, nil
}

// Apply returns the source of m.File with m applied, gofmt-formatted.
func Apply(m Mutant) ([]byte, error) {
	fset, file, sites, err := load(m.File)
	if err != nil {
		return nil, err
	}
	if m.nth < 0 || m.nth >= len(sites) {
		return nil, fmt.Errorf("mutate: %s: site %d out of range", m.ID, m.nth)
	}
	s := sites[m.nth]
	if p := fset.Position(s.pos); p.Line != m.Line || p.Column != m.Col || s.op != m.Op {
		return nil, fmt.Errorf("mutate: %s no longer matches the file", m.ID)
	}
	s.apply()
	var buf bytes.Buffer
	if err := format.Node(&buf, fset, file); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ByID finds a mutant in a list.
func ByID(ms []Mutant, id string) (Mutant, bool) {
	for _, m := range ms {
		if m.ID == id {
			return m, true
		}
	}
	return Mutant{}, false
}

func load(path string) (*token.FileSet, *ast.File, []site, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, nil, err
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.ParseComments)
	if err != nil {
		return nil, nil, nil, err
	}
	return fset, file, collect(file), nil
}

// collect walks the file in source order and records every mutation site.
// Skipped on purpose: String methods (they only format text), and if
// statements that test a bug-zoo switch (mut.Has), which are test scaffolding.
func collect(file *ast.File) []site {
	var sites []site
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil || fd.Name.Name == "String" {
			continue
		}
		fn := fd.Name.Name
		if fd.Recv != nil && len(fd.Recv.List) == 1 {
			fn = recvName(fd.Recv.List[0].Type) + "." + fn
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.IfStmt:
				if isMutationSwitch(x.Cond) {
					return true
				}
				cond := x.Cond
				for _, val := range []string{"true", "false"} {
					val := val
					op := OpIfTrue
					if val == "false" {
						op = OpIfFalse
					}
					sites = append(sites, site{pos: x.Cond.Pos(), op: op, fn: fn,
						apply: func() { x.Cond = ast.NewIdent(val) },
						undo:  func() { x.Cond = cond },
						desc:  func(fs *token.FileSet) string { return "if " + src(fs, cond) + " -> if " + val }})
				}
			case *ast.BinaryExpr:
				if to, ok := flip[x.Op]; ok {
					op := opOf(x.Op)
					from := x.Op
					to := to
					sites = append(sites, site{pos: x.OpPos, op: op, fn: fn,
						apply: func() { x.Op = to },
						undo:  func() { x.Op = from },
						desc: func(fs *token.FileSet) string {
							return src(fs, x) + " -> " + strings.Replace(src(fs, x), " "+from.String()+" ", " "+to.String()+" ", 1)
						}})
				}
			case *ast.BlockStmt:
				sites = append(sites, removals(x.List, func(i int, s ast.Stmt) { x.List[i] = s }, fn)...)
			case *ast.CaseClause:
				sites = append(sites, removals(x.Body, func(i int, s ast.Stmt) { x.Body[i] = s }, fn)...)
			}
			return true
		})
		// Off-by-one needs to replace the expression inside its parent, so it
		// is a second pass over every expression slot that can hold x±1.
		sites = append(sites, offByOne(fd.Body, fn)...)
	}
	sort.SliceStable(sites, func(i, j int) bool { return sites[i].pos < sites[j].pos })
	return sites
}

var flip = map[token.Token]token.Token{
	token.LSS: token.LEQ, token.LEQ: token.LSS, token.GTR: token.GEQ, token.GEQ: token.GTR,
	token.EQL: token.NEQ, token.NEQ: token.EQL,
	token.LAND: token.LOR, token.LOR: token.LAND,
}

func opOf(t token.Token) string {
	switch t {
	case token.EQL, token.NEQ:
		return OpNegate
	case token.LAND, token.LOR:
		return OpLogical
	}
	return OpBoundary
}

func removals(list []ast.Stmt, set func(int, ast.Stmt), fn string) []site {
	var out []site
	for i, st := range list {
		removable := false
		switch s := st.(type) {
		case *ast.IncDecStmt:
			removable = true
		case *ast.AssignStmt:
			removable = s.Tok != token.DEFINE // removing := would leave names undefined
		}
		if !removable {
			continue
		}
		i, st := i, st
		out = append(out, site{pos: st.Pos(), op: OpRemove, fn: fn,
			apply: func() { set(i, &ast.EmptyStmt{Semicolon: st.Pos(), Implicit: true}) },
			undo:  func() { set(i, st) },
			desc:  func(fs *token.FileSet) string { return "remove: " + src(fs, st) }})
	}
	return out
}

// offByOne finds x+1 and x-1 (literal 1 on the right) and replaces them by x.
func offByOne(body *ast.BlockStmt, fn string) []site {
	var out []site
	// replaceIn visits every field of n that holds an expression and offers
	// a site when that expression is x±1.
	var visit func(n ast.Node)
	visit = func(n ast.Node) {
		ast.Inspect(n, func(c ast.Node) bool {
			forEachExprSlot(c, func(get func() ast.Expr, set func(ast.Expr)) {
				be, ok := get().(*ast.BinaryExpr)
				if !ok || (be.Op != token.ADD && be.Op != token.SUB) {
					return
				}
				if lit, ok := be.Y.(*ast.BasicLit); !ok || lit.Value != "1" {
					return
				}
				orig := be
				out = append(out, site{pos: be.OpPos, op: OpOffByOne, fn: fn,
					apply: func() { set(orig.X) },
					undo:  func() { set(orig) },
					desc:  func(fs *token.FileSet) string { return src(fs, orig) + " -> " + src(fs, orig.X) }})
			})
			return true
		})
	}
	visit(body)
	return out
}

// forEachExprSlot calls f for every direct child expression of n that can
// be replaced.
func forEachExprSlot(n ast.Node, f func(get func() ast.Expr, set func(ast.Expr))) {
	slot := func(p *ast.Expr) {
		if *p != nil {
			f(func() ast.Expr { return *p }, func(e ast.Expr) { *p = e })
		}
	}
	list := func(xs []ast.Expr) {
		for i := range xs {
			slot(&xs[i])
		}
	}
	switch x := n.(type) {
	case *ast.AssignStmt:
		list(x.Rhs)
	case *ast.ReturnStmt:
		list(x.Results)
	case *ast.CallExpr:
		list(x.Args)
	case *ast.BinaryExpr:
		slot(&x.X)
		slot(&x.Y)
	case *ast.IndexExpr:
		slot(&x.Index)
	case *ast.SliceExpr:
		slot(&x.Low)
		slot(&x.High)
		slot(&x.Max)
	case *ast.KeyValueExpr:
		slot(&x.Value)
	case *ast.CompositeLit:
		list(x.Elts)
	case *ast.ParenExpr:
		slot(&x.X)
	case *ast.ValueSpec:
		list(x.Values)
	}
}

func isMutationSwitch(e ast.Expr) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok && sel.Sel.Name == "Has" {
			if inner, ok := sel.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "mut" {
				found = true
			}
		}
		return !found
	})
	return found
}

func recvName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return recvName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		return recvName(t.X)
	}
	return "?"
}

func src(fs *token.FileSet, n ast.Node) string {
	var b bytes.Buffer
	if err := format.Node(&b, fs, n); err != nil {
		return "?"
	}
	s := strings.Join(strings.Fields(b.String()), " ")
	if len(s) > 120 {
		s = s[:117] + "..."
	}
	return s
}
