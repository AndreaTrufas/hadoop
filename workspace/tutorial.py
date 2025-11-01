from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[A-Za-zÀ-ÿ0-9_']+")

class MRWordCount(MRJob):

    def mapper(self, _, line):
        for w in WORD_RE.findall(line):
            yield w.lower(), 1

    def reducer(self, word, counts):
        yield word, sum(counts)

if __name__ == "__main__":
    MRWordCount.run()