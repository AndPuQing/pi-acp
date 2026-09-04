import { execFileSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const readJson = async (relativePath) =>
  JSON.parse(await readFile(path.join(root, relativePath), 'utf8'));

const main = await readJson('package.json');
const platforms = [
  { id: 'linux-x64', name: 'pi-acp-rs-linux-x64', os: ['linux'], cpu: ['x64'] },
  { id: 'linux-arm64', name: 'pi-acp-rs-linux-arm64', os: ['linux'], cpu: ['arm64'] },
  { id: 'darwin-x64', name: 'pi-acp-rs-darwin-x64', os: ['darwin'], cpu: ['x64'] },
  { id: 'darwin-arm64', name: 'pi-acp-rs-darwin-arm64', os: ['darwin'], cpu: ['arm64'] },
  { id: 'win32-x64', name: 'pi-acp-rs-win32-x64', os: ['win32'], cpu: ['x64'] },
];
const errors = [];

let cargoMetadata;
try {
  cargoMetadata = JSON.parse(
    execFileSync('cargo', ['metadata', '--no-deps', '--format-version', '1'], {
      cwd: root,
      encoding: 'utf8',
    }),
  );
} catch (error) {
  errors.push(`cargo metadata failed: ${error.message}`);
}

const cargoPackage = cargoMetadata?.packages?.find((pkg) => pkg.name === 'pi-acp');
if (!cargoPackage) {
  errors.push('Cargo package pi-acp was not found.');
} else if (cargoPackage.version !== main.version) {
  errors.push(`version mismatch: Cargo is ${cargoPackage.version}, npm is ${main.version}.`);
}

for (const platform of platforms) {
  if (main.optionalDependencies?.[platform.name] !== main.version) {
    errors.push(`${platform.name} must be an optional dependency at version ${main.version}.`);
  }

  const manifest = await readJson(`npm/platforms/${platform.id}/package.json`);
  if (manifest.name !== platform.name) {
    errors.push(`npm/platforms/${platform.id}/package.json has the wrong name.`);
  }
  if (manifest.version !== main.version) {
    errors.push(`npm/platforms/${platform.id}/package.json is not at version ${main.version}.`);
  }
  if (JSON.stringify(manifest.os) !== JSON.stringify(platform.os)) {
    errors.push(`npm/platforms/${platform.id}/package.json has the wrong os constraint.`);
  }
  if (JSON.stringify(manifest.cpu) !== JSON.stringify(platform.cpu)) {
    errors.push(`npm/platforms/${platform.id}/package.json has the wrong cpu constraint.`);
  }
}

if (errors.length > 0) {
  console.error(errors.join('\n'));
  process.exitCode = 1;
} else {
  console.log(`npm package manifests match pi-acp-rs ${main.version}.`);
}
