import { chmod, cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const [assetDirArg, outputDirArg = 'npm-dist'] = process.argv.slice(2);

if (!assetDirArg) {
  console.error('Usage: node npm/scripts/prepare-release.mjs <asset-dir> [output-dir]');
  process.exit(1);
}

const assetDir = path.resolve(root, assetDirArg);
const outputDir = path.resolve(root, outputDirArg);
const mainManifest = JSON.parse(await readFile(path.join(root, 'package.json'), 'utf8'));
const platforms = [
  { id: 'linux-x64', asset: 'pi-acp-linux-x64', binary: 'pi-acp' },
  { id: 'linux-arm64', asset: 'pi-acp-linux-arm64', binary: 'pi-acp' },
  { id: 'darwin-x64', asset: 'pi-acp-macos-x64', binary: 'pi-acp' },
  { id: 'darwin-arm64', asset: 'pi-acp-macos-arm64', binary: 'pi-acp' },
  { id: 'win32-x64', asset: 'pi-acp-windows-x64.exe', binary: 'pi-acp.exe' },
];

await rm(outputDir, { recursive: true, force: true });
await mkdir(outputDir, { recursive: true });

const mainDir = path.join(outputDir, 'main');
await mkdir(mainDir, { recursive: true });
const publishedMainManifest = { ...mainManifest };
delete publishedMainManifest.scripts;
await writeFile(
  path.join(mainDir, 'package.json'),
  `${JSON.stringify(publishedMainManifest, null, 2)}\n`,
);
await cp(path.join(root, 'README.md'), path.join(mainDir, 'README.md'));
await cp(path.join(root, 'npm/bin'), path.join(mainDir, 'npm/bin'), { recursive: true });
await cp(path.join(root, 'npm/lib'), path.join(mainDir, 'npm/lib'), { recursive: true });

for (const platform of platforms) {
  const packageDir = path.join(outputDir, platform.id);
  const binaryDir = path.join(packageDir, 'bin');
  const assetPath = path.join(assetDir, platform.asset);
  await mkdir(binaryDir, { recursive: true });
  await cp(
    path.join(root, 'npm/platforms', platform.id, 'package.json'),
    path.join(packageDir, 'package.json'),
  );
  await cp(assetPath, path.join(binaryDir, platform.binary));
  await chmod(path.join(binaryDir, platform.binary), 0o755);
}

console.log(`Prepared npm packages for pi-acp ${mainManifest.version} in ${outputDir}.`);
