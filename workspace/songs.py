from mrjob.job import MRJob

class MRSongCount(MRJob):
    def mapper(self, _, line):
        try:
            artist, song = line.split(",", 1)
            artist = artist.strip().title()
            yield artist, 1
        except ValueError:
            pass  # línea malformada

    def reducer(self, artist, counts):
        yield artist, sum(counts)

if __name__ == "__main__":
    MRSongCount.run()
