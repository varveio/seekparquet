#!/usr/bin/env python3
"""Self-tests of check_docs.py; standard library only."""
import tempfile
import subprocess
import builtins
import symtable
import unittest
from pathlib import Path

import check_docs


class Slug(unittest.TestCase):
    def test_github_slugs(self):
        self.assertEqual(check_docs.slug('7. What readers check, and when'), '7-what-readers-check-and-when')
        self.assertEqual(check_docs.slug('3.4 Extensions'), '34-extensions')
        self.assertEqual(check_docs.slug('`no_manifest`'), 'no_manifest')
        self.assertEqual(check_docs.slug('Go and [Java](x.md) names'), 'go-and-java-names')

    def test_anchors_in_code_and_comments_do_not_count(self):
        text = ('````md\n<a id="ghost"></a>\n```\n## Still code\n````\n'
                '<!-- <a id="hidden"></a> -->\n`<a id="inline"></a>`\n<a id="real"></a>\n')
        self.assertEqual(check_docs.anchors(text), {'real'})

    def test_fences_close_only_with_matching_delimiters(self):
        for opening, false_close, closing in [
            ('~~~~md', '~~~', '~~~~'),
            ('````md', '~~~', '````'),
            ('~~~md', '~~~ still code', '~~~~'),
        ]:
            with self.subTest(opening=opening, false_close=false_close):
                text = (f'{opening}\n{false_close}\n<a id="ghost"></a>\n'
                        f'{closing}\n<a id="real"></a>\n')
                self.assertEqual(check_docs.anchors(text), {'real'})

    def test_comment_markers_inside_fences_are_literal(self):
        text = '```html\n<!--\n```\n<a id="real"></a>\n-->\n'
        self.assertEqual(check_docs.anchors(text), {'real'})

    def test_fences_inside_comments_are_not_opened(self):
        text = '<!--\n```\n<a id="hidden"></a>\n-->\n<a id="real"></a>\n'
        self.assertEqual(check_docs.anchors(text), {'real'})

    def test_duplicate_headings_and_anchors(self):
        text = '# A\n## A\n```\n# not a heading\n```\n- <a id="key-1"></a>[KEY-1] x\n'
        self.assertEqual(check_docs.anchors(text), {'a', 'a-1', 'key-1'})


class Checks(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.root = Path(self.dir.name)
        (self.root / 'spec').mkdir()
        (self.root / 'docs' / 'dev').mkdir(parents=True)
        (self.root / 'spec' / 'format-1.md').write_text(
            '# Spec\n\nRetired identifiers: LIST-1 (gone).\n\n- <a id="key-1"></a>[KEY-1] keys.\n- [KEY-2] no anchor.\n')
        (self.root / 'docs' / 'dev' / 'terms.txt').write_text('scope: docs/a.md\nbehaviour => behavior\n')

    def tearDown(self):
        self.dir.cleanup()

    def write(self, name, text):
        path = self.root / name
        path.write_text(text)
        return path

    def test_links(self):
        a = self.write('docs/a.md', '[ok](../spec/format-1.md#key-1) [bad](../spec/format-1.md#key-9) '
                                    '[gone](missing.md) [web](https://example.com) `[code](nowhere.md)`\n')
        problems = check_docs.check_links([a], self.root)
        self.assertEqual(len(problems), 2, problems)
        self.assertIn('#key-9', problems[0])
        self.assertIn('missing.md', problems[1])

    def test_links_to_code_and_comment_anchors_fail(self):
        self.write('docs/target.md', '```html\n<a id="ghost"></a>\n```\n'
                                   '<!--\n<a id="hidden"></a>\n-->\n<a id="real"></a>\n')
        a = self.write('docs/a.md', '[code](target.md#ghost) [comment](target.md#hidden) '
                                    '[real](target.md#real)\n')
        problems = check_docs.check_links([a], self.root)
        self.assertEqual(len(problems), 2, problems)
        self.assertIn('#ghost', problems[0])
        self.assertIn('#hidden', problems[1])

    def test_markdown_scan_excludes_untracked_files(self):
        subprocess.run(['git', 'init', '-q', str(self.root)], check=True)
        tracked = self.write('docs/tracked.md', '# Tracked\n')
        self.write('docs/untracked.md', '[broken](missing.md)\n')
        subprocess.run(['git', '-C', str(self.root), 'add', 'docs/tracked.md'], check=True)
        self.assertEqual(check_docs.markdown_files(self.root), [tracked])

    def test_python_recipe_snippet_defines_its_globals(self):
        source = (check_docs.ROOT / 'docs/examples/pyarrow_writer.py').read_text()
        snippet = source.split('# tag::recipe[]\n', 1)[1].split('# end::recipe[]', 1)[0]
        symbols = symtable.symtable(snippet, 'recipe', 'exec')
        unresolved = [s.get_name() for s in symbols.get_symbols()
                      if s.is_referenced() and not (s.is_imported() or s.is_assigned()
                                                   or hasattr(builtins, s.get_name()))]
        self.assertEqual(unresolved, [], 'the displayed recipe must define or import its globals')

    def test_requirements(self):
        a = self.write('docs/a.md', 'KEY-1, LIST-1 and KEY-7, UTF-8.\n')
        problems = check_docs.check_requirements([a], self.root)
        self.assertEqual(len(problems), 2, problems)
        self.assertIn('KEY-2 has no', problems[0])
        self.assertIn('KEY-7 is not', problems[1])

    def test_terms(self):
        a = self.write('docs/a.md', 'Its behaviour.\n`behaviour` in code.\nbehaviour <!-- terms: ok -->\n')
        b = self.write('docs/b.md', 'Out of scope behaviour.\n')
        problems = check_docs.check_terms([a, b], self.root)
        self.assertEqual(len(problems), 1, problems)
        self.assertIn('docs/a.md:1', problems[0])


class DocumentationContracts(unittest.TestCase):
    """Keep the reviewed cross-language and storage contracts visible in the docs."""

    def read(self, name):
        return (check_docs.ROOT / name).read_text()

    def test_changed_object_is_an_environment_error(self):
        for name in ('docs/concepts.md', 'docs/guides/troubleshooting.md'):
            with self.subTest(page=name):
                page = self.read(name)
                for text in ('ErrObjectChanged', 'storage.PartChangedException',
                             'never a refusal', 'reopen the dataset'):
                    self.assertIn(text, page)
                for text in ('ErrObjectTooLarge', '*LimitError', 'ManifestBytes'):
                    self.assertIn(text, page)

    def test_go_storage_capabilities_and_defaults(self):
        page = self.read('docs/concepts.md')
        for text in ('SizedSource.OpenSized', 'ManifestReader.ReadManifest', 'ErrObjectTooLarge',
                     '*LimitError', 'ManifestBytes', 'CacheMode', 'CacheDefault', 'CacheBypass',
                     'WithCacheMode', 'OpenOptions.OpenStats',
                     '| `Concurrency` | 64 |',
                     '| `MetadataPrefetchBytes` | 2 MiB; 64 KiB with `OpenDir` |',
                     '| `DisableSpeculation` | `false` |'):
            with self.subTest(contract=text):
                self.assertIn(text, page)
        self.assertNotIn('> Pending:', page)

    def test_cache_budget_allows_eviction(self):
        glossary = self.read('docs/concepts.md').split('## Glossary', 1)[1]
        self.assertIn('evicts cached bytes or stops keeping what it loads', glossary)

    def test_verify_checks_key_page_indexes(self):
        for name in ('docs/concepts.md', 'docs/guides/validating.md'):
            with self.subTest(page=name):
                self.assertIn("each non-empty row group's key page index and first key page",
                              self.read(name))

    def test_writedir_makes_directory_entries_durable(self):
        page = self.read('docs/guides/producing.md')
        self.assertIn("root's entry in its parent and `data/`'s entry in the root", page)
        self.assertIn('durable before publishing any part', page)

    def test_readme_distinguishes_shipped_storage(self):
        self.assertIn('A local directory implementation ships; an object store is an implementation you write.',
                      self.read('README.md'))

    def test_examples_use_current_names(self):
        self.assertNotIn('claude/reader-io-scheduling', self.read('docs/dev/docs.md'))
        for name in ('go/README.md', 'java/README.md'):
            with self.subTest(page=name):
                self.assertNotIn('Dataset.Verify(ctx)`', self.read(name))
        self.assertIn('Dataset.Verify(ctx, st)', self.read('java/README.md'))

    def test_go_macos_ci_is_path_selective(self):
        page = self.read('docs/reference/compatibility.md')
        self.assertIn('macOS CI runs selectively by changed paths', page)


    def test_input_row_spans_contract(self):
        source = self.read('go/rows.go')
        for text in ("caller's original input row spans", 'including overlaps and',
                     "plan's normalized union", 'A shard of Plan.Split'):
            self.assertIn(text, source)
        parity = self.read('docs/data/parity.json')
        self.assertIn('"go": "Plan.InputRowSpans", "java": "Plan.inputRowSpans"', parity)
        self.assertNotIn('func (plan *Plan) RowSpans()', source)

    def test_counting_requires_boundary_and_part_evidence(self):
        for name in ('go/README.md', 'docs/concepts.md', 'docs/guides/counting.md', 'go/CHANGELOG.md'):
            with self.subTest(page=name):
                page = self.read(name)
                for text in ('exact zero-request count for every listing entry',
                             'unchecked at plan construction', 'Dataset.Plan', 'zero'):
                    self.assertIn(text, page)
        guide = self.read('docs/guides/counting.md')
        for text in ('Cursor-clipped starts identify the first remaining candidate row',
                     'A listing `End` inside a common prefix', 'byte successor',
                     'Unknown boundaries must never be treated as empty spans or zero counts'):
            self.assertIn(text, guide)

    def test_row_positions_remain_inspectable_after_close(self):
        source = self.read('go/rows.go')
        self.assertIn('after Dataset.Close', source)
        self.assertNotIn('until their Dataset closes', source)
        guide = self.read('docs/guides/counting.md')
        self.assertIn('Positions remain inspectable after `Close`', guide)
        self.assertIn('`SelectRows` remain usable as in-memory plan operations after close', guide)
        self.assertIn('`Split` can read key pages for repeated keys', guide)

    def test_row_span_zero_is_invalid_input(self):
        source = self.read('go/rows.go')
        for text in ('Equal known boundaries', 'zero value is incomplete and invalid as input',
                     'Unknown boundaries are legitimate listing output',
                     'Span, whose zero value selects every key'):
            self.assertIn(text, source)

    def test_value_batch_ownership_and_grouping(self):
        source = self.read('go/valuescan.go')
        for text in ('Copying a ValueBatch or', 'does not retain values',
                     'independently owned position', 'cross adjacent input-span boundaries',
                     'Only batches produced by ScanValues carry data'):
            self.assertIn(text, source)
        guide = self.read('docs/guides/counting.md')
        for text in ('Copying a `ValueBatch` or `ColumnView` does not retain values',
                     'distribute each row by ordinal', 'start + int64(i)',
                     'sums[j] += column.Int64(i)'):
            self.assertIn(text, guide)

    def test_row_ordinals_are_not_batch_indexes(self):
        source = self.read('go/valuescan.go')
        for text in ('ordinals and counts are int64', 'indexes and lengths are int',
                     'start + int64(i)', 'without converting start to int'):
            self.assertIn(text, source)
        self.assertIn('32-bit and 64-bit systems', self.read('docs/guides/counting.md'))

    def test_shared_scan_docs_include_value_scans(self):
        self.assertIn('Plan.ScanValues early with a nil error', self.read('go/batch.go'))
        source = self.read('go/value.go')
        self.assertIn('projection slot of a Batch or', source)
        self.assertIn('Scan or ScanValues callback', source)
        self.assertIn('List, Plan, PlanRows, BuildListIndex or Verify', self.read('go/stats.go'))
        self.assertIn('A read through keys is routed', self.read('go/doc.go'))
        self.assertIn('Explicitly projecting the key column requests its values', self.read('go/stats.go'))

    def test_listing_comments_describe_generic_keys(self):
        source = self.read('go/query.go').split('// ListQuery is', 1)[1].split('// IsPrefix reports', 1)[0]
        self.assertNotIn('S3', source)
        self.assertNotIn('folder object', source)
        self.assertIn("row's key is the prefix itself", source)


if __name__ == '__main__':
    unittest.main()
