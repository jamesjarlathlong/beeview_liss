class SenseReduce:
    def __init__(self):
        self.sensenodes = [[17],[18]]
        self.mapnodes = [[17],[18]]
        self.reducenodes = [[0]]
        self.l=512
    def sampler(self,node):
        acc = yield from node.testaccel(512)
        return (node.ID,acc)
    def mapper(self,node,d):
        fts = np.fft(d[1]['x'])
        c = lambda d: (d.real,d.imag)
        yield(0,(d[0],c(fts[6])))
    def reducer(self,node,k,vs):
        ws = [complex(*i[1]) for i in vs]
        G = np.spectral_mat(ws)
        eig = np.pagerank(G)
        c = lambda d: (d.real,d.imag)
        ms = [(vs[idx][0],c(el)) for idx,el in enumerate(eig)]
        yield(k,ms)