'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { getPlatformSpec } = require('../lib/platform.js');

test('maps each release target to its platform package', () => {
  assert.equal(getPlatformSpec('linux', 'x64').packageName, '@earendil-works/pi-acp-linux-x64');
  assert.equal(getPlatformSpec('linux', 'arm64').packageName, '@earendil-works/pi-acp-linux-arm64');
  assert.equal(getPlatformSpec('darwin', 'x64').packageName, '@earendil-works/pi-acp-darwin-x64');
  assert.equal(getPlatformSpec('darwin', 'arm64').packageName, '@earendil-works/pi-acp-darwin-arm64');
  assert.equal(getPlatformSpec('win32', 'x64').binaryPath, 'bin/pi-acp.exe');
});

test('rejects unsupported release targets', () => {
  assert.throws(
    () => getPlatformSpec('freebsd', 'x64'),
    /Unsupported platform freebsd\/x64/,
  );
});
