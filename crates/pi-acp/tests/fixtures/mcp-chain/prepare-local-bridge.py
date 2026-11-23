#!/usr/bin/env python3
"""Stage a local bridge artifact in a fresh, credential-free test agent dir.

No global install, normal Pi profile access, provider call, or native mutation.
The generated endpoint descriptor uses only pi-acp's supported environment.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bridge-package', required=True, type=Path)
    parser.add_argument('--expected-bridge-sha256')
    parser.add_argument('--pi', required=True, type=Path)
    parser.add_argument('--endpoint', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    package, native, endpoint = (p.resolve(strict=True) for p in
                                 (args.bridge_package, args.pi, args.endpoint))
    output = args.output.absolute()
    if args.expected_bridge_sha256 and digest(package) != args.expected_bridge_sha256:
        parser.error('bridge artifact SHA-256 does not match the expected candidate')
    if output.exists():
        parser.error('output must be a new directory; prior setups stay immutable')
    npm, node = shutil.which('npm'), shutil.which('node')
    if not npm or not node:
        parser.error('Node and npm must be on PATH')
    version = subprocess.check_output([str(native), '--version'], text=True).strip()
    if version != '1.5.2':
        parser.error('this scenario requires native Pi 1.5.2')
    with tarfile.open(package, 'r:gz') as archive:
        metadata = json.load(archive.extractfile('package/package.json'))
    if metadata.get('name') != 'pi-mcp-adapter':
        parser.error('expected an npm-packed pi-mcp-adapter artifact')

    output.mkdir(parents=True)
    agent = output / 'agent'
    env_dir = agent / 'npm'
    env_dir.mkdir(parents=True)
    (env_dir / 'package.json').write_text(json.dumps({
        'name': 'pi-acp-local-bridge-integration', 'private': True,
    }) + '\n')
    # Avoid consulting user npm credentials/configuration during provisioning.
    user_config, global_config = output / 'npm-user.conf', output / 'npm-global.conf'
    user_config.write_text('')
    global_config.write_text('')
    with (output / 'npm-install.log').open('w') as log:
        subprocess.run([
            npm, 'install', '--ignore-scripts', '--no-audit', '--no-fund',
            '--legacy-peer-deps', '--userconfig', str(user_config),
            '--globalconfig', str(global_config), str(package), 'tsx', 'typebox',
            '@earendil-works/pi-coding-agent@1.5.2',
            '@earendil-works/pi-tui@1.5.2', '@earendil-works/pi-ai@1.5.2',
        ], cwd=env_dir, stdout=log, stderr=subprocess.STDOUT, check=True)
    installed = env_dir / 'node_modules/pi-mcp-adapter'
    installed_metadata = json.loads((installed / 'package.json').read_text())
    if installed_metadata['version'] != metadata['version']:
        raise RuntimeError('installed bridge differs from packed artifact')
    # tsx cannot type-strip TypeScript inside node_modules. This immutable
    # source copy resolves its dependencies from the same isolated env_dir.
    adapter_src = env_dir / 'adapter-src'
    shutil.copytree(installed, adapter_src)
    extension_dir = agent / 'extensions'
    extension_dir.mkdir()
    entry = (installed / 'index.ts').as_posix()
    (extension_dir / 'local-bridge.ts').write_text(
        'export { default } from ' + json.dumps(entry) + ';\n')
    (agent / 'settings.json').write_text(json.dumps({
        'extensions': ['-builtin:mcp'],
    }, indent=2) + '\n')
    endpoint_config = {
        'endpointId': 'local-modern-mcp-integration', 'acpProfile': 'alpha.4',
        'command': ['/usr/bin/env', 'PI_OFFLINE=1',
                    'PI_CODING_AGENT_DIR=' + str(agent),
                    'PI_ACP_PI_COMMAND=' + str(native),
                    'PI_ACP_ENABLE_MCP=true',
                    'PI_ACP_MCP_HTTP_PROTOCOL_VERSION=2026-07-28', str(endpoint)],
    }
    (output / 'endpoint.json').write_text(json.dumps(endpoint_config, indent=2) + '\n')
    sources = {str(path.relative_to(adapter_src)): digest(path)
               for path in sorted(adapter_src.rglob('*'))
               if path.is_file() and (path.suffix in ('.ts', '.js', '.mjs')
                                      or path.name == 'package.json')}
    manifest = {
        'schemaVersion': 1, 'bridgePackage': str(package),
        'bridgePackageSha256': digest(package),
        'bridgeVersion': installed_metadata['version'],
        'envDir': str(env_dir), 'adapterSrc': str(adapter_src),
        'node': str(Path(node).resolve()), 'nativePi': str(native),
        'nativePiVersion': version, 'nativePiSha256': digest(native),
        'endpoint': str(endpoint), 'endpointSha256': digest(endpoint),
        'agentDir': str(agent), 'bridgeSourceSha256': sources,
        'hostManagedModule': str(installed / 'host-managed.ts'),
        'packageLockSha256': digest(env_dir / 'package-lock.json'),
        'providerCalls': 0, 'normalProfileTouched': False,
        'credentialFilesReadBySetup': False, 'globalInstallationChanged': False,
        'scope': 'local deterministic integration setup; no authentication or admission claim',
    }
    (output / 'setup.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(output / 'setup.json')


if __name__ == '__main__':
    main()
