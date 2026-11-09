"""Hand-derived checks of the generated-only qualifier's separate row model."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_independent import b64, exact_after, expected, paginate


class IndependentTest(unittest.TestCase):
    def test_group_offsets_and_delimiter(self):
        keys = [b'a', b'a', b'a/x', b'a/y', b'b', b'c//x']
        values = [[{'int': str(i)}] for i in range(6)]
        self.assertEqual(expected(keys, values, delimiter=b'/'), [
            {'kind': 'row', 'key_base64': b64(b'a'), 'offset': 0, 'values': values[0]},
            {'kind': 'row', 'key_base64': b64(b'a'), 'offset': 1, 'values': values[1]},
            {'kind': 'prefix', 'key_base64': b64(b'a/')},
            {'kind': 'row', 'key_base64': b64(b'b'), 'offset': 0, 'values': values[4]},
            {'kind': 'prefix', 'key_base64': b64(b'c/')}])
        self.assertEqual(expected(keys, values, prefix=b'c/', delimiter=b'/'), [
            {'kind': 'prefix', 'key_base64': b64(b'c//')}])
        self.assertEqual(expected(keys, values, prefix=b'a', end=b'a/y', delimiter=b'/')[-1],
                         {'kind': 'prefix', 'key_base64': b64(b'a/')})

    def test_rejects_lost_duplicate_offset(self):
        model = expected([b'a', b'a'], [[], []])
        def broken(request):
            return {'status': 'ok', 'entries': [model[0]][:request['limit']]}
        with self.assertRaises(AssertionError):
            paginate(broken, Path('/unused'), model)

    def test_rejects_repeated_prefix(self):
        model = expected([b'a/x', b'b'], [[], []], delimiter=b'/')
        def broken(request):
            return {'status': 'ok', 'entries': model[:request['limit']]}
        with self.assertRaises(AssertionError):
            paginate(broken, Path('/unused'), model, delimiter=b'/')

    def test_exact_rows(self):
        keys = [b'a', b'b', b'b', b'c']
        values = [[{'int': str(i)}] for i in range(4)]
        self.assertEqual(expected(keys, values, delimiter=b'b'), [
            {'kind': 'row', 'key_base64': b64(b'a'), 'offset': 0, 'values': values[0]},
            {'kind': 'prefix', 'key_base64': b64(b'b'), 'exact': {'offset': 0, 'values': values[1]}},
            {'kind': 'row', 'key_base64': b64(b'c'), 'offset': 0, 'values': values[3]}])

    def test_rejects_missing_exact_rows(self):
        keys = [b'a', b'b', b'b', b'b/x', b'c']
        values = [[{'int': str(i)}] for i in range(5)]
        model = expected(keys, values, delimiter=b'b')
        self.assertIn('exact', model[1])
        stripped = [{k: v for k, v in e.items() if k != 'exact'} for e in model]
        served = []
        def broken(request):  # the correct pages, with every exact member dropped
            if 'after' not in request:
                served.clear()
            page = stripped[len(served):len(served) + request['limit']]
            served.extend(page)
            return {'status': 'ok', 'entries': page}
        with self.assertRaisesRegex(AssertionError, 'exact|prefix'):
            paginate(broken, Path('/unused'), model, delimiter=b'b')
        self.assertEqual(len(served), 2, 'the failure must be the stripped prefix entry')
        def correct_after(request):
            return {'status': 'ok', 'entries': [{'kind': 'prefix', 'key_base64': b64(b'b'),
                                                 'exact': {'offset': 1, 'values': values[2]}}]}
        exact_after(correct_after, Path('/unused'), keys, values, offset=0)
        def group_start(request):  # the group's first row instead of the first after the cursor
            return {'status': 'ok', 'entries': [{'kind': 'prefix', 'key_base64': b64(b'b'),
                                                 'exact': {'offset': 0, 'values': values[1]}}]}
        for broken_after in (group_start, lambda request: {'status': 'ok', 'entries': [{'kind': 'prefix', 'key_base64': b64(b'b')}]}):
            with self.assertRaises(AssertionError):
                exact_after(broken_after, Path('/unused'), keys, values, offset=0)


if __name__ == '__main__':
    unittest.main()
