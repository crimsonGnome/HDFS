from mrjob.job import MRJob
from mrjob.protocol import TextProtocol

class JoinMapClass(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, key, value):
        # super().mapper(key, value)
        itr = value.split()
        cwid = itr[0]
        # print("CWID : " + cwid)
        yield cwid, value

if __name__ == '__main__':
    JoinMapClass.run()