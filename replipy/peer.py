# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import flask


class ReplicationBlueprint(flask.Blueprint):
    pass


replipy = ReplicationBlueprint('replipy', __name__)
