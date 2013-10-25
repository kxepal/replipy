# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from flask import Flask
from replipy.peer import replipy

app = Flask(__name__)
app.register_blueprint(replipy)

if __name__ == '__main__':
    app.run(debug=True)
