#!/usr/bin/env node
// The hook entry, as one file a harness can name directly:
//
//   aop-send.cjs <harness> [event-name] [--chain "<command>"]
//   aop-send.cjs --flush            (the detached sender, started by a hook)
//
// Everything lives in lib/send.cjs; this file exists so a hook command and the
// detached flusher have a stable path to start from. Always exits 0.
'use strict';
require('../lib/send.cjs').run(process.argv.slice(2));
