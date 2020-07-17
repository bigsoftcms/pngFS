import pyfuse3
import pyfuse3_asyncio
import asyncio

from argparse import ArgumentParser

pyfuse3_asyncio.enable()


class pngFS(pyfuse3.Operations):
    
    def __init__(self):
        super().__init__()


def parse_args():
    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str)

    return parser.parse_args()


def main():
    args = parse_args()

    pngfs = pngFS()

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=pngFS')

    pyfuse3.init(pngfs, args.mountpoint, fuse_options)

    asyncio.run(pyfuse3.main())

    pyfuse3.close()


if __name__ == '__main__':
    main()
