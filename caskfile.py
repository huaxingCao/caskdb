import time
import struct
import os

"""
存储在磁盘中的格式
 _____________________________________________________________
|             |            |            |           |         |
|  timestamp  | key size   | value size |   key     |  value  |
|_____________|____________|____________|___________|_________|
"""

# 使用struct打包时的格式，定义了 timestamp、key size 和 value size 的字节顺序、大小和对齐方式
# > 代表小端，这是字节顺序。使用此代码时，请使用 sys.byteorder 来检查你的系统字节顺序。
# q 代表整数，8字节
# d 代表double类型的数
# 参考文档：https://docs.python.org/3/library/struct.html
METADATA_STRUCT_FORMAT = "<dqq"

# timestamp、key size 和 value size所占用的存储空间大小
METADATA_BYTE_SIZE = 24


class CaskFile:
    """
    caskdb存储的文件类
    """

    def __init__(self, dir, filename):
        self.dir = dir
        self.filename = filename
        self.path = os.path.join(dir, filename)
        self.load_next_entry_offset = 0

    def write(self, key, value):
        ts = time.time()
        key_sz = len(key)
        value_sz = len(value)
        # 数据以二进制的方式存储在磁盘文件当中，写入前需要转为二进制，
        # 方式是使用包 struct 的方法和字符串的 encode 方法。
        metadata = struct.pack(METADATA_STRUCT_FORMAT, ts, key_sz, value_sz)
        data = metadata + key.encode() + value.encode()
        with open(self.path, 'ab') as f:
            size = f.write(data)
            current_offset = f.tell()

        return ts, current_offset - size, size

    def read(self, offset, size):
        with open(self.path, 'rb') as f:
            f.seek(offset)
            data = f.read(size)
        metadata = data[:METADATA_BYTE_SIZE]
        kv_data = data[METADATA_BYTE_SIZE:]
        ts, key_size, value_size = struct.unpack(METADATA_STRUCT_FORMAT, metadata)
        key = kv_data[:key_size]
        value = kv_data[key_size:]

        return ts, key_size, value_size, key, value

    def reset_offset(self):
        self.load_next_entry_offset = 0

    def load_next_entry(self):
        is_done = False
        with open(self.path, 'rb') as f:
            offset = self.load_next_entry_offset
            f.seek(self.load_next_entry_offset)
            metadata = f.read(METADATA_BYTE_SIZE)
            if metadata == b'':
                ts, key_size, value_size = 0, 0, 0
                self.load_next_entry_offset = 0
                size = 0
                is_done = True
            else:
                ts, key_size, value_size = struct.unpack(METADATA_STRUCT_FORMAT, metadata)
                key = f.read(key_size).decode()
                value = f.read(value_size).decode()
                self.load_next_entry_offset = f.tell()
                size = METADATA_BYTE_SIZE + key_size + value_size

        return ts, offset, size, key, value, is_done

    def getFileSize(self):
        fsize = os.path.getsize(self.path)
        return round(fsize/(1024*1024))