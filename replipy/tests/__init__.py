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


class ReplipyTestCase(unittest.TestCase):

    def setUp(self):
        app.config['TESTING'] = True
        self.app = app.test_client()

    def tearDown(self):
        app.blueprints['replipy'].dbs.clear()

    def decode(self, rv):
        assert rv.content_type == 'application/json'
        try:
            return json.loads(rv.data.decode('utf-8'))
        except Exception as err:
            self.fail('unable decode json response: %s' % err)
