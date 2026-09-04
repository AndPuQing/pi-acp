'use strict';

const PLATFORM_SPECS = Object.freeze({
  'linux-x64': Object.freeze({
    packageName: '@earendil-works/pi-acp-linux-x64',
    binaryPath: 'bin/pi-acp',
  }),
  'linux-arm64': Object.freeze({
    packageName: '@earendil-works/pi-acp-linux-arm64',
    binaryPath: 'bin/pi-acp',
  }),
  'darwin-x64': Object.freeze({
    packageName: '@earendil-works/pi-acp-darwin-x64',
    binaryPath: 'bin/pi-acp',
  }),
  'darwin-arm64': Object.freeze({
    packageName: '@earendil-works/pi-acp-darwin-arm64',
    binaryPath: 'bin/pi-acp',
  }),
  'win32-x64': Object.freeze({
    packageName: '@earendil-works/pi-acp-win32-x64',
    binaryPath: 'bin/pi-acp.exe',
  }),
});

function getPlatformSpec(platform = process.platform, arch = process.arch) {
  const key = `${platform}-${arch}`;
  const spec = PLATFORM_SPECS[key];
  if (!spec) {
    const supported = Object.keys(PLATFORM_SPECS).join(', ');
    throw new Error(`Unsupported platform ${platform}/${arch}; supported targets: ${supported}.`);
  }
  return spec;
}

module.exports = { PLATFORM_SPECS, getPlatformSpec };
