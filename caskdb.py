import os
from keymap import KeyMap
from caskfile import CaskFile
import uuid

# 用一个随机值作为key被删除时存储的value
TOMBSTONE_VALUE = 'b28a5a93-437f-472f-af9e-6a757d3a033e'

MAX_FILE_SIZE = 10


class CaskDB:
    def __init__(self):
        self.dir = os.path.abspath('./data')
        os.makedirs(self.dir, exist_ok=True)

        # 初始化CaskDB时，用随机生成的id作为文件名，作为往CaskDB
        # 写入数据时，当前写入的数据文件。
        self.active_file = CaskFile(self.dir, str(uuid.uuid4()))

        # 所有的数据文件存储到file_map中，读取数据时使用对应的CaskFile进行读取
        self.file_map = {self.active_file.filename: self.active_file}
        for filename in os.listdir(self.dir):
            self.file_map[filename] = CaskFile(self.dir, filename)

        # 初始化key_map，读取所有的数据文件，将key载入key_map中。载入key时，
        # 将被删除的key放到deleted_key_map，然后删除key_map中对应的key
        self.key_map = KeyMap()
        deleted_key_map = {}
        for filename in self.file_map.keys():
            file = self.file_map[filename]
            file.reset_offset()
            is_done = False
            while not is_done:
                ts, offset, size, key, value, is_done = file.load_next_entry()
                if self.key_map.contain(key):
                    item = self.key_map.get(key)
                    if item.timestamp < ts:
                        self.key_map.put(key, filename, ts, offset, size)
                else:
                    self.key_map.put(key, filename, ts, offset, size)

                if value == TOMBSTONE_VALUE:
                    if key in deleted_key_map:
                        if deleted_key_map[key] < ts:
                            deleted_key_map[key] = ts
                    else:
                        deleted_key_map[key] = ts
        # key_map中删除在CaskDB中已经被删除的key
        for key in deleted_key_map.keys():
            if self.key_map.contain(key):
                if self.key_map.get(key).ts <= deleted_key_map[key]:
                    self.key_map.delete(key)

    def update_active_file(self):
        self.active_file = CaskFile(self.dir, str(uuid.uuid4()))
        self.file_map[self.active_file.filename] = self.active_file

    def put(self, key, value):
        ts, offset, size = self.active_file.write(key, value)
        self.key_map.put(key, self.active_file.filename, ts, offset, size)
        if self.active_file.getFileSize() >= MAX_FILE_SIZE:
            self.update_active_file()

    def get(self, key):
        item = self.key_map.get(key)
        if item:
            return self.file_map[item.filename].read(item.offset, item.size)

    def delete(self, key):
        self.key_map.delete(key)
        self.active_file.write(key, TOMBSTONE_VALUE)
        if self.active_file.getFileSize() >= MAX_FILE_SIZE:
            self.update_active_file()

    def contains(self, key):
        return self.key_map.contain(key)