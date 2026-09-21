"""Read the selected Pi installation's shipped settings metadata, without executing it.

Declarations or packaged settings docs expose override keys, not effective defaults. Unknown
or imported types stay JSON; native settings still support arbitrary future keys.
"""
import json
import os
import pathlib
import re
import shutil
import sys


def fields(source):
    source = re.sub(r'/\*.*?\*/|//[^\n]*', '', source, flags=re.S)
    interfaces = dict(re.findall(r'export interface (\w+)\s*\{([^{}]*)\}', source))
    aliases = dict(re.findall(r'export type (\w+)\s*=\s*([^;]+);', source))
    if 'Settings' not in interfaces:
        return []

    def choices(kind, seen=()):
        if kind in aliases and kind not in seen:
            return choices(aliases[kind].strip(), (*seen, kind))
        parts = [part.strip() for part in kind.split('|')]
        if all(re.fullmatch(r'"[^"\n]*"', part) for part in parts):
            return [json.loads(part) for part in parts]
        return []

    def walk(name, prefix='', seen=()):
        if name in seen:
            return []
        result = []
        for key, kind in re.findall(r'(\w+)\??\s*:\s*([^;]+);', interfaces[name]):
            key = prefix + key
            kind = kind.strip()
            if kind in interfaces:
                result.extend(walk(kind, key + '.', (*seen, name)))
            else:
                result.append({'key': key, 'type': kind, 'choices': choices(kind)})
        return result
    return walk('Settings')


def documented_fields(source):
    """Packaged apps may strip .d.ts files but retain version-matched docs."""
    if "## All Settings" not in source:
        return []
    result = {}
    table = False
    fenced = False
    for line in source.split("## All Settings", 1)[1].splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
            table = False
        if fenced:
            continue
        columns = [part.strip() for part in line.strip().strip('|').split('|')]
        if columns[:2] == ['Setting', 'Type']:
            table = True
            continue
        if not line.startswith('|'):
            table = False
        if not table or len(columns) < 4:
            continue
        match = re.fullmatch(r'`([A-Za-z][\w.]*)`', columns[0])
        kind = columns[1]
        if not match or not re.fullmatch(r'(?:string|number|boolean|object|array|integer)(?:\[\])?', kind):
            continue
        key = match.group(1)
        if any(not part for part in key.split('.')):
            continue
        # Defaults and examples are deliberately never parsed as user values.
        result[key] = {'key': key, 'type': kind, 'choices': []}
    return list(result.values())


def catalog(selected):
    executable = selected if pathlib.Path(selected).is_file() else shutil.which(selected)
    if not executable:
        return None
    binary = pathlib.Path(executable).resolve()
    for directory in list(binary.parents)[:6]:
        package = directory / 'package.json'
        if not package.is_file() or package.stat().st_size > 1024 * 1024:
            continue
        metadata = json.loads(package.read_text())
        if not isinstance(metadata, dict):
            return None
        if metadata.get('name') not in ('@earendil-works/pi-coding-agent', '@mariozechner/pi-coding-agent'):
            continue
        for relative, parse in [('dist/core/settings-manager.d.ts', fields), ('docs/settings.md', documented_fields)]:
            declaration = directory / relative
            if declaration.is_file() and declaration.stat().st_size <= 1024 * 1024:
                discovered = parse(declaration.read_text())
                if discovered:
                    version = metadata.get('version')
                    source = str(declaration) + (f' (Pi {version})' if isinstance(version, str) else '')
                    return {'source': source, 'fields': discovered}
        return None
    return None


def main():
    # Set only by the Local Studio adapter from its resolved native CLI command.
    selected = os.environ.get('ALLEYCAT_PI_SETTINGS_CLI') or sys.argv[1]
    try:
        result = catalog(selected)
    except (OSError, ValueError, TypeError, KeyError):
        result = None
    print(json.dumps(result))


if __name__ == '__main__':
    main()
