import pyfuse3
import pyfuse3_asyncio
import asyncio
import os
import errno
import stat
import pngdata
import pickle

from time import time_ns
from argparse import ArgumentParser

pyfuse3_asyncio.enable()


class SingleShotTimer:
    __slots__ = ['_callback', '_delay', '_task']

    def __init__(self, callback, delay):
        self._callback = callback
        self._delay = delay
        self._task = None

    async def _run(self):
        await asyncio.sleep(self._delay)
        self._callback()
        self._task = None

    def start(self):
        if self._task is not None:
            self._task.cancel()
        self._task = asyncio.create_task(self._run())


class File:
    __slots__ = ['name', 'inode', 'size', 'content', 'mode', 'is_dir', 'parent']

    def __init__(self, name, parent_inode, content, is_dir):
        self.name = name
        self.inode = None
        self.parent = parent_inode
        self.is_dir = is_dir
        if is_dir:
            self.mode = (stat.S_IFDIR | 0o755)
            self.content = {}  # directory childrens
            self.size = 0
        else:
            self.mode = (stat.S_IFREG | 0o644)
            self.content = content if content else b''
            self.size = len(self.content)


class FS:
    __slots__ = ['stat', 'files', 'inodes_created']

    def __init__(self):
        self.stat = pyfuse3.EntryAttributes()

        t = time_ns()
        self.stat.st_atime_ns = t
        self.stat.st_ctime_ns = t
        self.stat.st_mtime_ns = t
        self.stat.st_gid = os.getgid()
        self.stat.st_uid = os.getuid()

        self.files = {}
        self.inodes_created = pyfuse3.ROOT_INODE

        root = File(b'..', 0, None, True)
        self.add_file(root)

    def add_file(self, file):
        file.inode = self.inodes_created
        self.files[file.inode] = file
        if file.parent:
            self.files[file.parent].content[file.name] = file.inode
        self.inodes_created += 1

    def getattr(self, inode):
        file = self.get_file(inode)
        self.stat.st_mode = file.mode
        self.stat.st_size = file.size
        self.stat.st_ino = file.inode

        return self.stat

    def getattr_from_file(self, file):
        self.stat.st_mode = file.mode
        self.stat.st_size = file.size
        self.stat.st_ino = file.inode

        return self.stat

    def get_file(self, inode):
        try:
            return self.files[inode]
        except KeyError:
            raise pyfuse3.FUSEError(errno.ENOENT)

    def get_inode(self, parent_inode, name):
        try:
            return self.files[parent_inode].content[name]
        except KeyError:
            raise pyfuse3.FUSEError(errno.ENOENT)

    def clean_files_without_parent(self):
        for inode, file in list(self.files.items()):
            if inode != pyfuse3.ROOT_INODE:
                if file.parent not in self.files:
                    del self.files[inode]


class pngFS(pyfuse3.Operations):
    __slots__ = ['written', 'files', 'png_file']

    def __init__(self, args):
        super().__init__()

        try:
            self.files = pickle.loads(pngdata.decode(args.png_file, False))
            self.files.clean_files_without_parent()
        except (FileNotFoundError, AttributeError, pickle.UnpicklingError):
            self.files = FS()

        self.png_file = args.png_file
        self.write_timer = SingleShotTimer(self.write_to_png, args.delay)
        if args.save_at_exit:
            self.write_timer.start = lambda: ''

    def write_to_png(self):
        pngdata.encode(pickle.dumps(self.files, 4), self.png_file)

    async def getattr(self, inode, ctx=None):
        return self.files.getattr(inode)

    async def lookup(self, parent_inode, name, ctx=None):
        inode = self.files.get_inode(parent_inode, name)
        return await self.getattr(inode)

    async def opendir(self, inode, ctx):
        file = self.files.get_file(inode)
        if file.is_dir:
            return inode
        else:
            raise pyfuse3.FUSEError(errno.ENOTDIR)

    async def readdir(self, inode, off, token):
        childs = list(self.files.get_file(inode).content.values())

        for i in filter(lambda i: i > off, childs):
            child = self.files.get_file(i)
            r = pyfuse3.readdir_reply(
                token, child.name,
                self.files.getattr_from_file(child), i
            )
            if not r:
                return

    async def mkdir(self, inode_parent, name, mode, ctx):
        directory = File(name, inode_parent, None, True)
        self.files.add_file(directory)

        self.write_timer.start()

        return self.files.getattr_from_file(directory)

    async def create(self, inode_parent, name, mode, flags, ctx):
        file = File(name, inode_parent, None, False)
        self.files.add_file(file)

        self.write_timer.start()

        return (
            pyfuse3.FileInfo(fh=file.inode),
            self.files.getattr_from_file(file)
        )

    async def open(self, inode, flags, ctx):
        return pyfuse3.FileInfo(fh=inode)

    async def read(self, inode, offset, length):
        data = self.files.get_file(inode).content

        return data[offset:offset+length]

    async def write(self, inode, offset, buf):
        file = self.files.get_file(inode)

        data = memoryview(file.content)
        file.content = bytes(data[:offset]) + buf + bytes(data[offset+len(buf):])
        file.size = len(file.content)

        self.write_timer.start()

        return len(buf)

    async def rmdir(self, parent_inode, name, ctx):
        parent = self.files.get_file(parent_inode)
        inode = parent.content[name]

        directory = self.files.get_file(inode)
        if directory.content:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)

        del parent.content[name]
        del self.files.files[inode]

        self.write_timer.start()

    async def unlink(self, parent_inode, name, ctx):
        parent = self.files.get_file(parent_inode)
        inode = parent.content[name]

        del self.files.files[inode]
        del parent.content[name]

        self.write_timer.start()

    async def rename(self, parent_inode_old, name_old, parent_inode_new, name_new, flags, ctx):
        old_parent = self.files.get_file(parent_inode_old)
        new_parent = self.files.get_file(parent_inode_new)

        inode = old_parent.content[name_old]
        del old_parent.content[name_old]

        file = self.files.get_file(inode)
        file.name = name_new
        new_parent.content[name_new] = inode

        self.write_timer.start()


def parse_args():
    parser = ArgumentParser()

    parser.add_argument('png_file', type=str)
    parser.add_argument('mountpoint', type=str)
    parser.add_argument('-e', '--save-at-exit', action='store_true',
                        default=False, dest='save_at_exit',
                        help='save to png file at exit')
    parser.add_argument('-d', '--delay', default=2, dest='delay', type=int,
                        help='delay between last file modification and saving to png')
    parser.add_argument('--debug-fuse', default=False, action='store_true',
                        help='show fuse debug log', dest='fuse_debug')

    return parser.parse_args()


def main():
    args = parse_args()

    pngfs = pngFS(args)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=pngFS')
    if args.fuse_debug:
        fuse_options.add('debug')

    pyfuse3.init(pngfs, args.mountpoint, fuse_options)

    try:
        asyncio.run(pyfuse3.main())
    except KeyboardInterrupt:
        pass

    pngfs.files.clean_files_without_parent()
    pngfs.write_to_png()

    pyfuse3.close()


if __name__ == '__main__':
    main()
