# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from distutils.core import setup

setup(
    name='replipy',
    version='0.0',
    packages=['replipy'],
    url='https://github.com/kxepal/replipy',
    license='MIT',
    author='Alexander Shorin',
    author_email='kxepal@gmail.com',
    description='CouchDB Replication Protocol implementation in Python',
    packages=['replipy', 'replipy.tests'],
    install_requires=['flask']
)
