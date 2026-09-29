# Headless remote JSON settings backend. Runs through ProcessLauncher, never a terminal.
import json, os, sys, tempfile


def sensitive(key):
    key = key.lower().replace('_', '').replace('-', '')
    return any(part in ('token', 'auth') for part in key.split('.')) or any(
        part in key for part in ('apikey', 'secret', 'password', 'credential',
        'authorization', 'accesstoken', 'refreshtoken', 'bearer', 'headers', 'env'))


def sanitize(value):
    if isinstance(value, dict):
        return {key: sanitize(item) for key, item in value.items() if not sensitive(key)}
    if isinstance(value, list):
        return [sanitize(item) for item in value]
    return value


def replace_visible(original, replacement):
    if isinstance(original, list) and sanitize(original) != original:
        if sanitize(original) != replacement:
            raise ValueError('Edit credential-bearing arrays in the native runtime')
        return original
    if isinstance(original, dict):
        if not isinstance(replacement, dict):
            if sanitize(original) != original:
                raise ValueError('Cannot replace a credential-bearing settings group')
        else:
            for key, value in original.items():
                if sensitive(key):
                    replacement[key] = value
                elif key in replacement:
                    replacement[key] = replace_visible(value, replacement[key])
                elif sanitize(value) != value:
                    raise ValueError('Cannot remove a credential-bearing settings group')
    return replacement


def merge(dest, source):
    if isinstance(dest, dict) and isinstance(source, dict):
        for key, value in source.items():
            dest[key] = merge(dest.get(key), value)
        return dest
    return source


def set_key(root, key, value, strategy):
    if not key or key.startswith('_') or any(not p or sensitive(p) for p in key.split('.')):
        raise ValueError('Invalid or sensitive setting key')
    if sanitize(value) != value:
        raise ValueError('Credential settings cannot be edited here')
    if not isinstance(root, dict):
        raise ValueError('Settings parent is not an object')
    if key in root or '.' not in key:
        root[key] = merge(root.get(key), value) if strategy == 'upsert' else replace_visible(root.get(key), value)
    else:
        head, tail = key.split('.', 1)
        set_key(root.setdefault(head, {}), tail, value, strategy)


def main():
    runtime, operation = sys.argv[1:3]
    if runtime == 'claude':
        root = os.environ.get('CLAUDE_CONFIG_DIR') or os.path.expanduser('~/.claude')
    elif runtime == 'pi':
        root = os.environ.get('PI_CODING_AGENT_DIR') or os.path.expanduser('~/.pi/agent')
    else:
        raise ValueError('Unsupported native settings runtime')
    path = os.path.realpath(os.path.join(root, 'settings.json'))
    def read():
        try:
            with open(path, encoding='utf-8') as stream:
                result = json.load(stream)
        except FileNotFoundError:
            result = {}
        if not isinstance(result, dict):
            raise ValueError('Native settings must be an object')
        return result
    if operation == 'read':
        print(json.dumps({'config': sanitize(read()), 'source': path}))
        return
    if operation != 'write':
        raise ValueError('Unsupported settings operation')
    params = json.loads(sys.argv[3])
    if params.get('filePath') not in (None, path) or params.get('expectedVersion') is not None:
        raise ValueError('Native settings file/version override is unsupported')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Serialize this backend's concurrent writers without locking the replaced inode.
    import fcntl
    with open(path + '.litter.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        current = read()
        for edit in params['edits']:
            key, value = edit['keyPath'], edit['value']
            if edit.get('mergeStrategy') not in ('replace', 'upsert'):
                raise ValueError('Unsupported merge strategy')
            if key == '$native':
                if not isinstance(value, dict) or sanitize(value) != value:
                    raise ValueError('Native settings require a credential-free object')
                current = replace_visible(current, value)
            else:
                set_key(current, key, value, edit['mergeStrategy'])
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=os.path.dirname(path), delete=False) as stream:
                temporary = stream.name
                if os.path.exists(path):
                    os.chmod(temporary, os.stat(path).st_mode & 0o777)
                json.dump(current, stream, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            temporary = None
            if read() != current:
                raise ValueError('Native settings read-back differs from write')
        finally:
            if temporary is not None:
                os.unlink(temporary)
    print(json.dumps({'status': 'ok', 'version': '', 'filePath': path}))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        # Never echo config contents or credential-bearing native errors.
        print(json.dumps({'error': 'Native settings operation failed; check the native configuration file and supported setting value'}))
        sys.exit(1)
