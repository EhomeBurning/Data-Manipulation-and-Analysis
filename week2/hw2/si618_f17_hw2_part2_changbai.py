#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 19 12:02:32 2017

@author: liuchangbai
"""


from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):

    def mapper_get_words(self, _, line):
        for word in WORD_RE.findall(line):
            counts = 1
            yield (word.lower(), counts)

    def combiner_count_words(self, word, counts):
        yield (word, sum(counts))

    def reducer_count_words(self, word, counts):
        yield (None, (sum(counts), word))

    def reducer_find_max_word(self, _, word_count_pairs):
        yield (max(word_count_pairs))
        
    def steps(self):
        return [
                MRStep(mapper = self.mapper_get_words,
                       combiner = self.combiner_count_words,
                       reducer = self.reducer_count_words),
                MRStep(reducer = self.reducer_find_max_word)
                ]

if __name__ == '__main__':
    MRMostUsedWord.run()


