"""Metadata discovery must stay bound to the selected installed package."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'src/pi_settings_schema.py'
module_spec = importlib.util.spec_from_file_location('pi_settings_schema', SCRIPT)
schema = importlib.util.module_from_spec(module_spec)
module_spec.loader.exec_module(schema)

# Native Pi0.84.2 shipped settings.md table shape; default values are sentinels.
DOC = '''# Settings
## All Settings
| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `theme` | string | `private-example` | Theme |
| `retry.enabled` | boolean | `true` | Retry |
| `retry.maxRetries` | number | `999999` | Retry count |
| `extensions` | string[] | - | Paths |
| `unknown` | ambiguous | - | Not a typed setting |

```json
{"madeUp": "must-not-appear"}
```
'''


class SchemaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'dist/core').mkdir(parents=True)
        (self.root / 'docs').mkdir()
        self.cli = self.root / 'dist/cli.js'
        self.cli.write_text('throw Error("metadata discovery must not execute CLI")')
        self.package = self.root / 'package.json'
        self.package.write_text(json.dumps({'name': '@earendil-works/pi-coding-agent', 'version': '0.84.2'}))
        (self.root / 'docs/settings.md').write_text(DOC)

    def test_packaged_docs_preserve_types_without_defaults_or_examples(self):
        result = schema.catalog(str(self.cli))
        self.assertIn('docs/settings.md (Pi 0.84.2)', result['source'])
        self.assertEqual({r['key']: r['type'] for r in result['fields']},
                         {'theme': 'string', 'retry.enabled': 'boolean', 'retry.maxRetries': 'number', 'extensions': 'string[]'})
        self.assertNotIn('private-example', json.dumps(result))
        self.assertNotIn('999999', json.dumps(result))
        self.assertNotIn('madeUp', json.dumps(result))

    def test_declarations_still_take_precedence(self):
        (self.root / 'dist/core/settings-manager.d.ts').write_text('export interface Settings { nativeOnly?: boolean; }')
        self.assertEqual(schema.catalog(str(self.cli))['fields'], [{'key': 'nativeOnly', 'type': 'boolean', 'choices': []}])

    def test_bundled_cli_locator_does_not_inspect_node_package(self):
        env = dict(os.environ, ALLEYCAT_PI_SETTINGS_CLI=str(self.cli))
        result = subprocess.run([sys.executable, str(SCRIPT), '/missing/node'], env=env, check=True, capture_output=True, text=True)
        self.assertEqual(len(json.loads(result.stdout)['fields']), 4)

    def test_foreign_package_and_malformed_metadata_fail_closed(self):
        self.package.write_text('{"name":"@oh-my-pi/pi-coding-agent"}')
        self.assertIsNone(schema.catalog(str(self.cli)))
        self.package.write_text('[]')
        self.assertIsNone(schema.catalog(str(self.cli)))
        self.package.write_text('{malformed')
        result = subprocess.run([sys.executable, str(SCRIPT), str(self.cli)], env={k:v for k,v in os.environ.items() if k != 'ALLEYCAT_PI_SETTINGS_CLI'}, check=True, capture_output=True, text=True)
        self.assertIsNone(json.loads(result.stdout))
        self.assertEqual(schema.documented_fields('| `fake` | boolean | true | example |'), [])


if __name__ == '__main__':
    unittest.main()
