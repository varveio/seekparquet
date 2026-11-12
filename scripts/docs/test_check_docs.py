#!/usr/bin/env python3
"""Self-tests of check_docs.py; standard library only."""
import tempfile
import unittest
from pathlib import Path

import check_docs


class Slug(unittest.TestCase):
    def test_github_slugs(self):
        self.assertEqual(check_docs.slug('7. What readers check, and when'), '7-what-readers-check-and-when')
        self.assertEqual(check_docs.slug('3.4 Extensions'), '34-extensions')
        self.assertEqual(check_docs.slug('`no_manifest`'), 'no_manifest')
        self.assertEqual(check_docs.slug('Go and [Java](x.md) names'), 'go-and-java-names')

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


if __name__ == '__main__':
    unittest.main()
