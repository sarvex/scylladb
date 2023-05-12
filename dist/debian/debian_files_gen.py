#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2020-present ScyllaDB
#

#
# SPDX-License-Identifier: AGPL-3.0-or-later
#

import string
import os
import shutil
import re
import subprocess
from pathlib import Path

class DebianFilesTemplate(string.Template):
    delimiter = '%'

scriptdir = os.path.dirname(__file__)

changelog_template = Path(
    os.path.join(scriptdir, 'changelog.template')
).read_text()
control_template = Path(
    os.path.join(scriptdir, 'control.template')
).read_text()
with open('build/SCYLLA-PRODUCT-FILE') as f:
    product = f.read().strip()

with open('build/SCYLLA-VERSION-FILE') as f:
    version = f.read().strip().replace('-', '~')

with open('build/SCYLLA-RELEASE-FILE') as f:
    release = f.read().strip()

if os.path.exists('build/debian/debian'):
    shutil.rmtree('build/debian/debian')
shutil.copytree('dist/debian/debian', 'build/debian/debian')

if product != 'scylla':
    for p in Path('build/debian/debian').glob('scylla-*'):
        # pat1: scylla-server.service
        #    -> scylla-enterprise-server.scylla-server.service
        #       or
        #       scylla-server.default
        #    -> scylla-enterprise-server.scylla-server.default
        # pat2: scylla-server.scylla-fstrim.service
        #    -> scylla-enterprise-server.scylla-fstrim.service
        # pat3: scylla-conf.install
        #    -> scylla-enterprise-conf.install

        if m := re.match(r'^scylla(-[^.]+)\.(service|default)$', p.name):
            p.rename(p.parent / f'{product}{m[1]}.{p.name}')
        elif m := re.match(r'^scylla(-[^.]+\.scylla-[^.]+\.[^.]+)$', p.name):
            p.rename(p.parent / f'{product}{m[1]}')
        else:
            p.rename(p.parent / p.name.replace('scylla', product, 1))

s = DebianFilesTemplate(changelog_template)
changelog_applied = s.substitute(product=product, version=version, release=release, revision='1', codename='stable')

s = DebianFilesTemplate(control_template)
control_applied = s.substitute(product=product)

with open('build/debian/debian/changelog', 'w') as f:
    f.write(changelog_applied)

with open('build/debian/debian/control', 'w') as f:
    f.write(control_applied)

include_binaries = subprocess.run("./scripts/create-relocatable-package.py --print-libexec -", shell=True, check=True, capture_output=True, encoding='utf-8').stdout
with open('build/debian/debian/source/include-binaries', 'w') as f:
    f.write(include_binaries)
