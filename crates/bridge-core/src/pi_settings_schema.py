"""Read the selected Pi installation's shipped Settings declaration, without executing it.

Declarations expose supported user override keys, not effective defaults. Unknown
or imported types stay JSON; native settings still support arbitrary future keys.
"""
import json
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


def main():
    executable = shutil.which(sys.argv[1])
    if not executable:
        print('null')
        return
    binary = pathlib.Path(executable).resolve()
    for directory in list(binary.parents)[:6]:
        package = directory / 'package.json'
        if not package.is_file():
            continue
        name = json.loads(package.read_text()).get('name', '')
        if name not in ('@earendil-works/pi-coding-agent', '@mariozechner/pi-coding-agent'):
            continue
        declaration = directory / 'dist/core/settings-manager.d.ts'
        if declaration.is_file() and declaration.stat().st_size <= 1024 * 1024:
            print(json.dumps({'source': str(declaration), 'fields': fields(declaration.read_text())}))
            return
    print('null')


if __name__ == '__main__':
    main()
