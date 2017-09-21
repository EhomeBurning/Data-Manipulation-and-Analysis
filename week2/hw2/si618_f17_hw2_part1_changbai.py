#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 11:04:58 2017

@author: liuchangbai
"""

from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):
    def mapper(self, _, line):
        for w in WORD_RE.findall(line):
            counts = 1
            yield (w.lower(), counts)
            
    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        yield (word, sum(counts))
            
if __name__ == "__main__":
    MRMostUsedWord.run()