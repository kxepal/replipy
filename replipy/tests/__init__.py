# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import json
import unittest
from replipy import app
from replipy.storage import MemoryDatabase


class ReplipyTestCase(unittest.TestCase):

    def setUp(self):
        app.config['TESTING'] = True
        app.blueprints['replipy'].db_cls = MemoryDatabase
        self.app = app.test_client()

    def tearDown(self):
        app.blueprints['replipy'].dbs.clear()

    def decode(self, rv):
        assert rv.content_type == 'application/json'
        try:
            return json.loads(rv.data.decode('utf-8'))
        except Exception as err:
            self.fail('unable decode json response: %s' % err)

    def encode(self, data):
        return json.dumps(data)


class ReplipyDBTestCase(ReplipyTestCase):

    def setUp(self):
        super(ReplipyDBTestCase, self).setUp()
        self.dbname = 'replipy'
        self.app.put('/%s/' % self.dbname, content_type='application/json')
