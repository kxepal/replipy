# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

import time
from abc import ABCMeta


class ABCDatabase(object, metaclass=ABCMeta):

    def __init__(self, name):
        self._name = name
        self._start_time = int(time.time() * 10**6)
        self._update_seq = 0

    @property
    def name(self):
        """Returns database symbolic name as string"""
        return self._name

    @property
    def start_time(self):
        """Returns database start time in microseconds"""
        return self._start_time

    @property
    def update_seq(self):
        """Returns current update sequence value"""
        return self._update_seq

    def info(self):
        """Returns database information object as dict"""
        return {
            'db_name': self.name,
            'instance_start_time': str(self.start_time),
            'update_seq': self.update_seq
        }
