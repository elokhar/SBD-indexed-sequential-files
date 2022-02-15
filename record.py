from sys import byteorder

INT_SIZE = 4
RECORD_SIZE = 5*INT_SIZE
NULL_INDEX = 0xffff

class record:
    
    key = 0
    c = 0
    m = 0
    dT = 0
    ov_index = NULL_INDEX

    def __init__(self, key, c, m, dT, ov_index=NULL_INDEX):
        self.key = key
        self.c = c
        self.m = m
        self.dT = dT
        self.ov_index = ov_index

    
    def get_key(self):
        return self.key

    def get_ov(self):
        return self.ov_index

    def set_ov(self, ov_index):
        self.ov_index = ov_index
    
    def __str__(self, show_ov = False):
        return("Record "+str(self.key)+": "+str(self.c)+" "+str(self.m)+" "+str(self.dT))

    def __bytes__(self):
        key_bytes = self.key.to_bytes(INT_SIZE, byteorder = "big")
        c_bytes = self.c.to_bytes(INT_SIZE, byteorder = "big")
        m_bytes = self.m.to_bytes(INT_SIZE, byteorder = "big")
        dT_bytes = self.dT.to_bytes(INT_SIZE, byteorder = "big")
        ov_bytes = self.ov_index.to_bytes(INT_SIZE, byteorder = "big")
        return key_bytes + c_bytes + m_bytes + dT_bytes + ov_bytes

    @staticmethod
    def to_bytes(self):
        return bytes(self)

    @classmethod
    def from_bytes(cls, record_bytes):
        key = int.from_bytes(record_bytes[:INT_SIZE], byteorder = "big")
        c = int.from_bytes(record_bytes[INT_SIZE+1:2*INT_SIZE], byteorder = "big")
        m = int.from_bytes(record_bytes[2*INT_SIZE+1:3*INT_SIZE], byteorder = "big")
        dT = int.from_bytes(record_bytes[3*INT_SIZE+1:4*INT_SIZE], byteorder = "big")
        ov = int.from_bytes(record_bytes[4*INT_SIZE+1:5*INT_SIZE], byteorder="big")
        return cls(key, c, m, dT, ov)


