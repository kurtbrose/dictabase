import pickle
import struct

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

class Data(object): 
    def __setattr__(self, key, val):
        object.__setattr__(self, key, val)
        #register to listen to changes
        if isinstance(val, Data):
            val.register(self, key)
        self.dirty()
    
    def __delattr__(self, key):
        object.__delattr__(key)
        self.dirty()        
    
    def register(self, other, key):
        getattr(self, "listeners", []).append((other, key))
    
    def dirty(self, sofar=None):
        root = False
        if sofar is None:
            sofar = set()
            root = True
        if self not in sofar:
            sofar.add(self)
            for listener, key in self.listeners:
                listener.dirty(sofar)
        if root:
            self.pheonix.dirty(sofar)

def mark_dirty(f):
    def g(self, *a, **kw):
        ret = f(self, *a, **kw)
        self.dirty()
        return ret
    return g

class DataDict(Data, dict):
    pass

class DataList(Data, list):
    pass

class DataSet(Data, set):
    pass

def write_data(file, pickler, data):
    #pickle the data, storing it into a temporary StringIO object
    sfile = StringIO()
    pickler.write = sfile.write
    pickler.dump(data)
    output = sfile.getvalue()
    #write the length of the pickled data, followed by the pickled data
    self.file.write(struct.pack('L', len(output)))
    self.file.write(output)

def read_data(file, unpickler):
    while True:
        size = file.read(4)
        if size == '':
            raise StopIteration()
        size = struct.unpack("L", size)
        unpickler.read = StringIO(file.read(size)).read
        yield unpickler.load()

class PheonixStore(object):
    def __init__(self, file):
        self.file = file
        self.pickler = pickle.Pickler(file)
        self.unpickler = pickle.Unpickler(file)
        self.data = {}
        #read self.data from the current file
        for path, value in read_data(file, unpickler):
            if len(path) == 1:
                self.data[path[0]] = value
                value.pheonix = self
                value.pheonix_key = path[0]
            else:
                cur = self.data[path[0]]
                for p in path[1:-1]: cur = getattr(cur, p)
                setattr(cur, path[-1], value)
    
    def dirty(self, dirtyset):
        for data in dirtyset:
            self.memo.rev_del(data)
        for data in dirtyset:
            path = []
            cur = data
            while hasattr(cur, "listeners") and len(cur.listeners) > 0:
                path.append(cur.listeners[0][1])
                cur = cur.listeners[0][0]
            path.append(cur.pheonix_key)
            path.reverse()
            write_data(self.file, self.pickler, (tuple(path), data))
    
    def save(self):
        self.pickler.dump(self.data.items())

    def __getitem__(self, key):
        return self.data[key]
        
    def __setitem__(self, key, item):
        item.pheonix = self
        item.pheonix_key = key
        self.data[key] = item
        write_data(self.file, self.pickler, (key, item))
        
    def __delitem__(self, key):
        del self.data[key]
        write_data(self.file, self.pickler, (key,))



'''
Basic strategy is this:
Repeatedly call Pickle, allowing the memo to stick around.

On the other side, repeatedly call Unpickle, again allowing the memo
to stick around as it builds up.

How often should it save?  On every assignment?
'''




