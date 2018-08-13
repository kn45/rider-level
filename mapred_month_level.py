#!/usr/bin/env python

from __future__ import print_function
import json
import pymr
import sys


def month_diff(m1, m2):
    # m1 = '201805' m2 = '201807'
    # diff = 2
    y1 = int(m1[:4])
    y2 = int(m2[:4])
    return (y2 - y1) * 12 + int(m2[4:6]) - int(m1[4:6])


class MyReducer(pymr.Reducer):
    def _setup(self):
        self.min_mon = sys.argv[2]
        self.max_mon = sys.argv[3]
        self.month_len = month_diff(self.min_mon, self.max_mon) + 1
        self.score = json.load(open('score.json'))
        self.score = [x[1] for x in self.score]
        self.dim = len(self.score)
        self.level = json.load(open('level.json'))
        self.keep_th, self.decay_rate = json.load(open('threshold.json'))

    def _reduce(self, key, values):
        res = [[0] * (len(self.level) + 1) for _ in range(self.month_len)]
        for value in values:
            flds = value.split('\t')
            flds = flds[-self.month_len:]
            for i, val in enumerate(flds):
                res[i][int(val)] += 1
        for i, levels in enumerate(res):
            print(key, i, *levels, sep='\t')


if __name__ == '__main__':
    if sys.argv[1] == 'm':
        MyMapper().run()
    if sys.argv[1] == 'r':
        MyReducer().run()
