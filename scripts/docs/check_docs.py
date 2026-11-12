#!/usr/bin/env python3
"""Documentation checks; standard library only.

Run from anywhere: python3 scripts/docs/check_docs.py

- links: every relative link in every Markdown file resolves to a file or
  directory, and every #fragment to a heading or an <a id> in its target;
- requirement ids: every requirement of spec/format-1.md has its <a id>
  anchor, and every requirement id a Markdown file cites exists;
- terminology: the pages in scope (docs/dev/terms.txt) use none of the
  denied terms outside code.

Generated pages and snippets are checked by go/internal/tools/docgen.
"""
import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SKIP_DIRS = {'.git', 'build', '.gradle', 'node_modules', '.venv', 'datasets'}

FENCE = re.compile(r'^\s*(```|~~~)')
INLINE_CODE = re.compile(r'(`+)(?:(?!\1).)+\1')
LINK = re.compile(r'!?\[(?:[^\]\[]|\[[^\]]*\])*\]\(\s*<?([^)\s>]+)>?(?:\s+"[^"]*")?\s*\)')
REF_LINK = re.compile(r'^\s*\[[^\]]+\]:\s*(\S+)', re.M)
ANCHOR = re.compile(r'<a\s+(?:id|name)="([^"]+)"', re.I)
HEADING = re.compile(r'^(#{1,6})\s+(.*?)\s*#*\s*$')
REQ_DEF = re.compile(r'^- (?:<a id="([^"]+)"></a>)?\[([A-Z]+-[0-9]+)\]', re.M)
REQ_ID = re.compile(r'\b([A-Z]{2,7})-([0-9]+)\b')


def markdown_files(root=ROOT):
    """Tracked Markdown files; outside git, every one."""
    try:
        listed = subprocess.run(['git', '-C', str(root), 'ls-files', '-z', '--cached',
                                 '--', '*.md'], capture_output=True, check=True).stdout
        names = sorted({n for n in listed.decode().split('\0') if n and '/datasets/' not in '/' + n})
        return [root / n for n in names if (root / n).exists()]
    except (OSError, subprocess.CalledProcessError):
        pass
    out = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d not in SKIP_DIRS and not d.startswith('.'))
        for name in sorted(filenames):
            if name.endswith('.md'):
                out.append(Path(dirpath) / name)
    return out


def rendered_lines(text):
    """Yield (line number, line without inline code, line with it) outside fenced code and HTML comments.

    A fence closes only on a fence of the same character, at least as long, with nothing after it.
    """
    fence = None
    comment = False
    for number, line in enumerate(text.split('\n'), 1):
        if fence is not None:
            if re.fullmatch(r'\s*' + re.escape(fence[0]) + '{' + str(len(fence)) + r',}\s*', line):
                fence = None
            continue
        # Comments in a code fence are literal text. Outside fences, remove
        # comments with state across lines before looking for a new fence.
        visible = ''
        while line:
            if comment:
                _hidden, end, line = line.partition('-->')
                if not end:
                    break
                comment = False
            else:
                before, start, line = line.partition('<!--')
                visible += before
                if not start:
                    break
                comment = True
        line = visible
        m = FENCE.match(line)
        if m:
            fence = re.match(r'\s*(`{3,}|~{3,})', line).group(1)
            continue
        yield number, INLINE_CODE.sub('', line), line


def prose_lines(text):
    """Yield (line number, line) outside fenced code, without inline code and comments."""
    for number, line, _raw in rendered_lines(text):
        yield number, line


def slug(heading):
    """GitHub's anchor for a heading's text."""
    text = re.sub(r'!?\[([^\]]*)\]\([^)]*\)', r'\1', heading)  # links keep their text
    text = re.sub(r'<[^>]+>', '', text)                         # inline HTML
    text = text.replace('`', '').lower()
    text = re.sub(r'[^\w\- ]', '', text)
    return text.replace(' ', '-')


def anchors(text):
    """The fragments a Markdown text defines: heading slugs and <a id> anchors, outside code and comments."""
    found = set()
    seen = {}
    for _number, line, raw in rendered_lines(text):
        found.update(ANCHOR.findall(line))
        m = HEADING.match(raw)
        if not m:
            continue
        base = slug(m.group(2))
        n = seen.get(base, 0)
        seen[base] = n + 1
        found.add(base if n == 0 else f'{base}-{n}')
    return found


def check_links(files, root=ROOT):
    problems = []
    cache = {}

    def anchors_of(path):
        if path not in cache:
            cache[path] = anchors(path.read_text(encoding='utf-8'))
        return cache[path]

    for path in files:
        text = path.read_text(encoding='utf-8')
        targets = []
        for number, line in prose_lines(text):
            targets += [(number, t) for t in LINK.findall(line)]
        targets += [(text[:m.start()].count('\n') + 1, m.group(1)) for m in REF_LINK.finditer(text)]
        for number, target in targets:
            if re.match(r'^[a-z][a-z0-9+.-]*:', target, re.I):
                continue  # http:, https:, mailto: are checked elsewhere
            where = f'{path.relative_to(root)}:{number}'
            file_part, _, fragment = target.partition('#')
            dest = path if not file_part else (path.parent / file_part).resolve()
            if not dest.exists():
                problems.append(f'{where}: link to {target}: no such file')
                continue
            if fragment and dest.is_file() and dest.suffix == '.md' and fragment not in anchors_of(dest):
                problems.append(f'{where}: link to {target}: no such anchor')
    return problems


def check_requirements(files, root=ROOT):
    problems = []
    spec = (root / 'spec' / 'format-1.md').read_text(encoding='utf-8')
    defined = set()
    for anchor, rid in REQ_DEF.findall(spec):
        defined.add(rid)
        if anchor != rid.lower():
            problems.append(f'spec/format-1.md: requirement {rid} has no <a id="{rid.lower()}"></a> anchor')
    retired = set()
    m = re.search(r'Retired identifiers:([^.]*(?:\([^)]*\))?[^.]*)\.', spec)
    if m:
        retired = {'-'.join(x) for x in REQ_ID.findall(m.group(1))}
    prefixes = {rid.split('-')[0] for rid in defined}
    for path in files:
        for number, line in prose_lines(path.read_text(encoding='utf-8')):
            for prefix, n in REQ_ID.findall(line):
                rid = f'{prefix}-{n}'
                if prefix in prefixes and rid not in defined and rid not in retired:
                    problems.append(f'{path.relative_to(root)}:{number}: {rid} is not a requirement of spec/format-1.md')
    return problems


def load_terms(root=ROOT):
    scope, terms = [], []
    for raw in (root / 'docs' / 'dev' / 'terms.txt').read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('scope:'):
            scope.append(line[len('scope:'):].strip())
            continue
        pattern, _, advice = line.partition('=>')
        terms.append((re.compile(r'(?<![\w-])' + pattern.strip() + r'(?![\w-])', re.I), pattern.strip(), advice.strip()))
    return scope, terms


def in_scope(rel, scope):
    return any(rel == s or (s.endswith('/') and rel.startswith(s)) for s in scope)


def check_terms(files, root=ROOT):
    scope, terms = load_terms(root)
    problems = []
    for path in files:
        rel = path.relative_to(root).as_posix()
        if not in_scope(rel, scope):
            continue
        raw = path.read_text(encoding='utf-8').split('\n')
        for number, line in prose_lines(path.read_text(encoding='utf-8')):
            if 'terms: ok' in raw[number - 1]:
                continue
            line = re.sub(r'\]\([^)]*\)', ']', line)  # link targets are paths, not prose
            line = re.sub(r'<[^>]+>', '', line)         # nor are HTML tags and their ids
            for regex, pattern, advice in terms:
                m = regex.search(line)
                if m:
                    problems.append(f'{rel}:{number}: "{m.group(0)}": {advice}')
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.parse_args(argv)
    files = markdown_files()
    problems = check_links(files) + check_requirements(files) + check_terms(files)
    for p in problems:
        print(p)
    print(f'{len(files)} Markdown files, {len(problems)} problems')
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main())
