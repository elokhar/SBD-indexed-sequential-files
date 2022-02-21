from sys import byteorder

INT_SIZE = 4
RECORD_SIZE = 5*INT_SIZE
NULL_INDEX = -1

class record:
    


    def __init__(self, key, c, m, dT, ov_index=NULL_INDEX, deleted=0):
        self.key = key
        self.c = c
        self.m = m
        self.dT = dT
        self.ov_index = ov_index
        self.deleted = deleted

    
    def get_key(self):
        return self.key

    def get_ov(self):
        return self.ov_index

    def set_ov(self, ov_index):
        self.ov_index = ov_index

    def has_ov(self):
        if self.ov_index != NULL_INDEX:
            result = True
        else:
            result = False
        return result

    def clear_ov(self):
        self.ov_index = NULL_INDEX

    def mark_deleted(self):
        self.deleted = 1

    def check_deleted(self):
        if self.deleted == 1:
            result = True
        else:
            result = False
        return result
    
    def __str__(self, show_ov = False):
        if self.check_deleted():
            is_deleted_str = "deleted"
        else:
            is_deleted_str = ""
        if self.has_ov():
            ov_str = " ov="+str(self.get_ov())
        else:
            ov_str = ""
        return("Record "+str(self.key)+": "+str(self.c)+" "+str(self.m)+" "+str(self.dT)+ov_str+" "+is_deleted_str)

    def __bytes__(self):
        key_bytes = self.key.to_bytes(INT_SIZE, byteorder = "big")
        c_bytes = self.c.to_bytes(INT_SIZE, byteorder = "big")
        m_bytes = self.m.to_bytes(INT_SIZE, byteorder = "big")
        dT_bytes = self.dT.to_bytes(INT_SIZE, byteorder = "big")
        ov_bytes = self.ov_index.to_bytes(INT_SIZE-1, byteorder = "big", signed = True)
        del_bytes = self.deleted.to_bytes(1, byteorder = "big")
        return key_bytes + c_bytes + m_bytes + dT_bytes + ov_bytes + del_bytes

    @staticmethod
    def to_bytes(self):
        return bytes(self)

    @classmethod
    def from_bytes(cls, record_bytes):
        key = int.from_bytes(record_bytes[:INT_SIZE], byteorder = "big")
        c = int.from_bytes(record_bytes[INT_SIZE+1:2*INT_SIZE], byteorder = "big")
        m = int.from_bytes(record_bytes[2*INT_SIZE+1:3*INT_SIZE], byteorder = "big")
        dT = int.from_bytes(record_bytes[3*INT_SIZE+1:4*INT_SIZE], byteorder = "big")
        ov = int.from_bytes(record_bytes[4*INT_SIZE+1:5*INT_SIZE-1], byteorder = "big", signed = True)
        deleted = int.from_bytes(record_bytes[5*INT_SIZE-1:5*INT_SIZE], byteorder = "big")
        return cls(key, c, m, dT, ov, deleted)


