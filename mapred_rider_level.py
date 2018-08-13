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


class MyMapper(pymr.Mapper):
    def _map(self, inl):
        flds = inl.split('\t')
        print('|'.join(flds[:2]), *flds[2:], sep='\t')


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

    def score2level(self, score):
        for lvl in range(len(self.level)):
            if self.level[lvl] > score:
                return lvl
        return len(self.level)

    def mon_level(self, last_score, scores):
        this_score = sum([x[0] * x[1] for x in zip(scores, self.score)])
        total_score = last_score + this_score
        this_level = self.score2level(total_score)
        if self.keep_th[this_level] > scores[0]:
            total_score *= self.decay_rate[this_level]
            this_level = self.score2level(total_score)
        return total_score, this_level

    def _reduce(self, key, values):
        cid, rid = key.split('|')
        data = [[0.0] * self.dim] * self.month_len
        for value in values:
            flds = value.split('\t')
            mon = flds[0]
            data[month_diff(self.min_mon, mon)] = map(float, flds[1:])
        output = []
        last_score = 0.0
        for scores in data:
            score, level = self.mon_level(last_score, scores)
            output.append((score, level))
            last_score = score
        print(cid, rid,
              '\t'.join([str(x[0]) for x in output]),
              '\t'.join([str(x[1]) for x in output]), sep='\t')

if __name__ == '__main__':
    if sys.argv[1] == 'm':
        MyMapper().run()
    if sys.argv[1] == 'r':
        MyReducer().run()
