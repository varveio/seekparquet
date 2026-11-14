// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	seekparquet "github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/testutil/fakestore"
	"github.com/varveio/seekparquet/go/local"
	"github.com/varveio/seekparquet/go/write"
)

// folderFixture writes 64 folders root/gNN/fNNN/ of per objects, eight to
// a gNN/, in two key-only parts of groups row groups each, with 64-row
// pages and the key encoded as enc ("plain", "dict" or "delta"): the
// shapes of the #81 review's listing matrix (issue 82).
func folderFixture(t *testing.T, per, groups int, enc string) *ioFixture {
	t.Helper()
	var keys []string
	for f := range 64 {
		for i := range per {
			keys = append(keys, fmt.Sprintf("root/g%02d/f%03d/obj-%05d.dat", f/8, f, i))
		}
	}
	slices.Sort(keys)
	var node parquet.Node = parquet.String()
	switch enc {
	case "plain":
		node = parquet.Encoded(node, &parquet.Plain)
	case "delta":
		node = parquet.Encoded(node, &parquet.DeltaByteArray)
	case "dict":
		node = parquet.Encoded(node, &parquet.RLEDictionary)
	}
	schema := parquet.NewSchema("folders", parquet.Group{"key": node})
	fx := &ioFixture{dir: filepath.Join(t.TempDir(), "folders"), objects: map[string]fakestore.Object{}}
	must(t, os.MkdirAll(filepath.Join(fx.dir, "data"), 0o755))
	root, err := os.OpenRoot(fx.dir)
	must(t, err)
	defer root.Close()
	ops := local.Ops{Root: root, AllowUnsynced: true}
	key := seekparquet.Key{Column: "key", Unique: true}
	var infos []seekparquet.PartInfo
	for p := range 2 {
		begin, end := p*len(keys)/2, (p+1)*len(keys)/2
		tmp, final := fmt.Sprintf("data/.part-%05d.tmp", p), fmt.Sprintf("data/part-%05d.parquet", p)
		file, err := root.Create(tmp)
		must(t, err)
		w, err := write.NewPart(file, schema, write.PartOptions{Key: key, SchemaID: "test/folders/v1", PageRows: 64,
			RowGroupRows: int64((end - begin + groups - 1) / groups), RowGroupBytes: 1 << 40})
		must(t, err)
		rows := make([]parquet.Row, end-begin)
		for i, k := range keys[begin:end] {
			rows[i] = parquet.Row{parquet.ByteArrayValue([]byte(k)).Level(0, 0, 0)}
		}
		_, err = w.WriteRows(rows)
		must(t, err)
		info, err := w.Seal()
		must(t, err)
		must(t, local.PublishPart(ops, tmp, final))
		info.Path = final
		infos = append(infos, info)
	}
	_, err = local.Commit(ops, seekparquet.Manifest{Key: key, SchemaID: "test/folders/v1", Parts: infos,
		Version: seekparquet.Version{FormatVersion: seekparquet.FormatVersion, DraftRevision: seekparquet.DraftRevision}})
	must(t, err)
	for _, name := range append([]string{seekparquet.ManifestName}, partPaths(infos)...) {
		data, err := os.ReadFile(filepath.Join(fx.dir, filepath.FromSlash(name)))
		must(t, err)
		fx.objects[name] = fakestore.Object{Data: data}
	}
	// Decode a page out of any synctest bubble first (ioFixtureOf).
	d, err := seekparquet.OpenDir(context.Background(), fx.dir, seekparquet.OpenOptions{})
	must(t, err)
	_, err = d.List(context.Background(), seekparquet.ListQuery{Limit: 1}, nil)
	must(t, errors.Join(err, d.Close()))
	return fx
}

// pagesScenario pages through prefix, limit entries a page with a count
// of each common prefix, following each page's cursor on one handle, for
// pages pages, each a step.
func pagesScenario(name, prefix string, limit, pages int) ioScenario {
	return ioScenario{name, func(e *ioEnv) {
		d := e.open()
		q := seekparquet.PageQuery{ListQuery: seekparquet.ListQuery{Prefix: []byte(prefix), Delimiter: '/', Limit: limit},
			Reductions: []seekparquet.Reduction{{Op: seekparquet.CountRows}}}
		for i := range pages {
			e.step(fmt.Sprintf("%s/page%d", name, i+1), func() string {
				page, err := d.Page(context.Background(), q, nil)
				var b strings.Builder
				for _, ent := range page.Entries {
					fmt.Fprintf(&b, "%q %v\n", ent.Bytes(), ent.Aggregates)
				}
				q.Cursor = page.Next
				return withErr(b.String(), err)
			})
		}
	}}
}

// The budgets of TestReadBudgetsListingShapes. Pages 2 and 3 resume from
// a cursor, with no scan position to route forward from: they route by
// the routing search, which reads what the first page's forward routing
// did not (row groups' first keys, the next part). In these shapes the
// work moves from the first page to the second, and over the three pages
// it is no more than before #78 (7264a9f), pinned as the targets' totals
// below; not every listing shape is so (issue 82).
var (
	// Dictionary keys, speculation off, root/g00/ one folder a page.
	// Before #78: 18/14, 3/3 and 3/3 requests/rounds (24/20, 22,425 B).
	ioDictFolderBudgets = []ioBudget{
		{"dict-folders/page1", ioMetrics{6, 5, 15138, 0, 2}, none, current},
		{"dict-folders/page2", ioMetrics{15, 12, 6013, 0, 2}, none, current},
		{"dict-folders/page3", ioMetrics{3, 3, 1282, 0, 1}, none, current},
	}
	// Delta keys, ten pages a folder, the root one gNN/ a page, with
	// speculation. Before #78: 5/5, 2/2 and 2/2 (9/9, 192,535 B).
	ioDeltaRootBudgets = []ioBudget{
		{"delta-root/page1", ioMetrics{3, 2, 95632, 0, 2}, none, current},
		{"delta-root/page2", ioMetrics{4, 3, 95818, 0, 2}, none, current},
		{"delta-root/page3", ioMetrics{2, 1, 436, 0, 2}, none, current},
	}
	// Plain keys, eight row groups a part, one gNN/ each: the root's
	// predicted pages lie in successive row groups, a few hundred bytes
	// apart. Before #78: 4 requests in 4 rounds; #78 to #81: 10 in 4.
	ioPlainGroupsBudgets = []ioBudget{
		{"plain-groups/page1", ioMetrics{4, 4, 15673, 0, 1}, ioTarget{4, 4}, atTarget},
	}
)

// TestReadBudgetsListingShapes measures listing shapes the #81 review
// found doing more work in a request than before #78 (issue 82), and
// checks that over each of these sequences of pages the work is no more
// than before #78: requests and rounds at most its totals.
func TestReadBudgetsListingShapes(t *testing.T) {
	seekparquet.NoTestReadAhead(t)
	for _, c := range []struct {
		fx               *ioFixture
		scenario         ioScenario
		store            ioStore
		table            []ioBudget
		requests, rounds int // before #78, over the pages
	}{
		{folderFixture(t, 96, 8, "dict"), pagesScenario("dict-folders", "root/g00/", 1, 3), noSpeculationStore, ioDictFolderBudgets, 24, 20},
		{folderFixture(t, 640, 1, "delta"), pagesScenario("delta-root", "root/", 1, 3), capableStore, ioDeltaRootBudgets, 9, 9},
		{folderFixture(t, 32, 8, "plain"), pagesScenario("plain-groups", "root/", 1000, 1), capableStore, ioPlainGroupsBudgets, 4, 4},
	} {
		reports := runIOScenarios(t, c.fx, []ioScenario{c.scenario}, []ioStore{c.store})[c.store.name]
		checkBudgets(t, c.scenario.name, c.table, reports)
		var requests, rounds int
		for _, r := range reports {
			s := r.Summary()
			requests, rounds = requests+s.Requests, rounds+s.Rounds
		}
		if requests > c.requests || rounds > c.rounds {
			t.Errorf("%s: %d requests in %d rounds over its pages, more than %d in %d before #78", c.scenario.name, requests, rounds, c.requests, c.rounds)
		}
	}
}
