"""Test-only conformance transport for the shipped validator API."""
import json
import sys
from seekparquet.validate import validate_dataset

for line in sys.stdin:
    req = json.loads(line)
    if req.get('op') == 'hello':
        response = {'status': 'ok', 'implementation': 'seekparquet-python-validator', 'version': '0.1.0',
                    'supports': [{'format_version': 0, 'draft_revision': 1}],
                    'targets': ['validator-' + x for x in ('structural', 'profile', 'full', 'digest')],
                    'capabilities': ['open'] + ['validate:' + x for x in ('structural', 'profile', 'full', 'digest')]}
    elif req.get('op') == 'validate':
        try:
            levels = validate_dataset(req['dataset'], req['level'])
            response = {'status': 'ok', 'levels': levels}
        except ValueError as e:
            response = {'status': 'invalid_request', 'message': str(e)}
    else:
        response = {'status': 'invalid_request', 'message': 'validate only'}
    print(json.dumps({'id': req.get('id'), **response}), flush=True)
