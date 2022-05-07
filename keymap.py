class KeyMap:
    """
    内存中的Hash Table，用于存储key对应value的位置
    """

    def __init__(self):
        self.map = {}

    def put(self, key, file, ts, offset, size):
        self.map[key] = KeyMapItem(file, ts, offset, size)

    def get(self, key):
        if self.map.__contains__(key):
            return self.map[key]

    def delete(self, key):
        if key in self.map:
            del self.map[key]

    def contain(self, key):
        return key in self.map


class KeyMapItem:
    """
    封装KeyMap中存储的数据
    """

    def __init__(self, filename, ts, offset, size):
        self.filename = filename
        self.timestamp = ts
        self.size = size
        self.offset = offset
