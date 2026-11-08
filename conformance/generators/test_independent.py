"""Hand-derived checks of the generated-only qualifier's separate row model."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_independent import b64, expected, paginate


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


if __name__ == '__main__':
    unittest.main()
