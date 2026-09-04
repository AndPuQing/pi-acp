#!/usr/bin/env node
'use strict';

const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const { getPlatformSpec } = require('../lib/platform.js');

function resolveBinary() {
  const spec = getPlatformSpec();
  let packageJsonPath;

  try {
    packageJsonPath = require.resolve(`${spec.packageName}/package.json`);
  } catch (error) {
    const missingPackage = new Error(
      `The optional dependency ${spec.packageName} is not installed. ` +
        'Reinstall pi-acp-rs without --omit=optional.',
    );
    missingPackage.cause = error;
    throw missingPackage;
  }

  const binaryPath = path.join(path.dirname(packageJsonPath), spec.binaryPath);
  if (!fs.existsSync(binaryPath)) {
    throw new Error(`The installed platform package is missing ${spec.binaryPath}.`);
  }

  return binaryPath;
}

let binaryPath;
try {
  binaryPath = resolveBinary();
} catch (error) {
  console.error(`pi-acp-rs: ${error.message}`);
  process.exitCode = 1;
}

if (binaryPath) {
  const child = spawn(binaryPath, process.argv.slice(2), {
    stdio: 'inherit',
    windowsHide: false,
  });

  child.once('error', (error) => {
    console.error(`pi-acp-rs: failed to start native binary: ${error.message}`);
    process.exitCode = 1;
  });

  child.once('exit', (code, signal) => {
    if (signal) {
      console.error(`pi-acp-rs: native binary exited on signal ${signal}`);
      process.exitCode = 1;
    } else {
      process.exitCode = code ?? 1;
    }
  });
}
