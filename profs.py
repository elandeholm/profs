import os
import sys
import argparse
import stat
import struct

class Paths():
	paths = set()
    
	def __init__(self, base=None, relative_only=True, paths_list=None):
		if base == None:
			self.base = '.'
		else:
			self.base = base

		self.base = os.path.realpath(self.base)
		self.relative_only = relative_only

		if paths_list == None:
			paths_list = [ ]

		for path in paths_list:
			self.add(path)

	def add(self, path):
		path = path.strip()
		try:
			canonical_path = os.path.realpath(path)
		except ValueError: # '\0'-terminated path
			non_zt_path = path[:path.index(chr(0))]
			canonical_path = os.path.realpath(path)

		if self.relative_only:
			if canonical_path.startswith(self.base):
				canonical_path = canonical_path[len(self.base)+1:]
			else:
				raise ValueError('{} is not under {}'.format(canonical_path, self.base))

		if not canonical_path:
			canonical_path = '.'
            
		self.paths.add(canonical_path)

	def __iter__(self):
		return iter(self.paths)

class StatObject():
	__slots__ = [ 'mode', 'inode', 'gid', 'uid', 'mtime', 'ctime', 'size', '_serialized' ]
	PACK_FMT = 'IIIIlll'

	def __repr__(self):
		return str({ key: getattr(self, key) for key in self.__slots__ })

	def __init__(self, data=None, stat_result=None, **kwargs):
		self._serialized = None

		if data is not None:
			self.unserialize(self, data)

		if stat_result is not None:
			self.mode  = stat_result.st_mode
			self.gid   = stat_result.st_gid
			self.uid   = stat_result.st_uid
			self.ctime = int(stat_result.st_ctime)
			self.mtime = int(stat_result.st_mtime)
			self.size  = stat_result.st_size
			
		for key, value in kwargs.items():
			if key in self.__slots__ and not key[0] == '_':
				setattr(self, key, value)
			else:
				raise KeyError(key)

	def isdir(self):
		return stat.S_ISDIR(self.mode)

	def isreg(self):
		return stat.S_ISREG(self.mode)

	def serialize(self):
		if self._serialized is None:
			self._serialized = struct.pack(
				self.PACK_FMT,
				self.mode, self.inode, self.gid, self.uid,
				self.mtime, self.ctime, self.size)
		return self._serialized

	def unserialize(self, data):
		(
			self.mode, self.inode, self.gid, self.uid,
			self.mtime, self.ctime, self.size
		) =	struct.unpack(self.PACK_FMT, data)
		
	def delta(self, other=None):
		serialized = self.serialize()
		if not other:
			return serialized
		else:
			z = zip(serialized, other.serialize())
			return bytes([ b1 ^ b2 for b1, b2 in z ])

class FileSystem():
	inode = 0
	names = { }
	stat_objects = [ ]

	def __repr__(self):
		return 'FileSystem(inode={}, names={})'.format(self.inode, self.names, self.stat_objects)

	def __init__(self, base, create, paths, follow, bailout, progress, maxsize, name=None):
		self.base = base
		if self.base == '/':
			self.base_len = 1
		else:
			self.base_len = len(base) + 1
		self.follow   = follow
		self.bailout  = bailout
		self.maxsize  = maxsize
		self.progress = progress
		self.name     = name

		if create:
			self.init_from_paths(paths=paths)
			self.export_fs()
		else:
			self.import_fs()

	def import_fs(self):
		with open(self.name, 'r') as f:
			l = f.readline()
			self.inode = int(l)

			for i in range(self.inode):
				name = f.readline().strip()
				inode = int(f.readline())
				self.names[name] = inode

			sos = f.read()
			for i in range(self.inode):
				data = sos[40*i:40*i + 40]
				self.stat_objects[i] = StatObject(data=data)

	def export_fs(self):
		with open(self.name, 'x') as f:
			f.buffer.write(bytes('{}\n'.format(self.inode), encoding='utf-8'))
			for name in self.names:
				assert '\n' not in self.names
				f.buffer.write(bytes('{}\n{}\n'.format(name, self.names[name]), encoding='utf-8'))

			l = []
			for so in self.stat_objects:
				print(so.serialize())
				
				l.append(so.serialize())

			f.buffer.write(b''.join(l))

	def new_node(self, name):
		if name in self.names:
			if self.bailout:
				raise ValueError('duplicate: {}'.format(name))
			else:
				return None
	
		inode = self.inode
		self.inode += 1
		self.names[name] = inode
		
		if self.progress and not self.inode % 1000:
			print('\r{}        '.format(self.inode), end='')
		
		return inode

	def init_from_paths(self, paths):
		canonical_path = os.path.abspath(self.base)				
		stat_result = os.stat(canonical_path, follow_symlinks=False)
		self.dev = stat_result.st_dev

		self._init_from_paths(cwd=None, paths=paths)
		if self.progress:
			print('\r{}        '.format(self.inode))

	def _init_from_paths(self, cwd, paths):
		try:
			maxsize = int(self.maxsize)
		except TypeError:
			maxsize = None
	
		for path in paths:
			try:
				relative_path = path
				if cwd is not None:
					relative_path = os.path.join(cwd, path)
				abs_path = os.path.join(base, relative_path)
				canonical_path = os.path.abspath(abs_path)				
				if canonical_path.startswith(self.base):
					name = canonical_path[self.base_len:]
				else:
					print('canonical_path {} does not start with {}'.format(canonical_path, self.base))
					assert False

				stat_result = os.stat(canonical_path, follow_symlinks=False)
				
				if stat_result.st_dev != self.dev:
					print('\rskipping node {}, other FS'.format(canonical_path))
				else:
					if maxsize is not None and stat.S_ISREG(stat_result.st_mode) and stat_result.st_size > maxsize:
						print('\rskipping file {}, too large ({})'.format(canonical_path, stat_result.st_size))
					else:
						inode = self.new_node(name)
						if inode is None:
							print('\rskipping name {} ({}, {}, {}), duplicate'.format(name, canonical_path, self.base, relative_path))
						else:
							so = StatObject(stat_result=stat_result, inode=inode)
							self.stat_objects.append(so)
					
					if stat.S_ISDIR(stat_result.st_mode):
						if self.follow:
							try:
								children = os.listdir(abs_path)
								self._init_from_paths(cwd=relative_path, paths=children)
							except FileNotFoundError:
								# race?
								assert False
							except PermissionError:
								print('\rskipping dir {}, permission error'.format(canonical_path))
			except FileNotFoundError:
				if self.bailout:
					raise

	def name_2_inode(self, name, accept_absolute=False):
		try:
			return self.names[name]
		except KeyError:
			if accept_absolute:
				if name.startswith(self.base):
					relative_name = name[self.base_len:]
					try:
						return self.names[relative_name]
					except KeyError:
						return None
				else:
					raise ValueError('{} is not under {}'.format(name, self.base))
			else:
				return None

	def inode_2_stat_object(self, inode=inode):
		return self.stat_objects[inode]

if __name__ == "__main__":
	arg_parser = argparse.ArgumentParser()
	arg_parser.add_argument('-', action='store_true', dest='from_stdin', help='take paths from stdin')
	arg_parser.add_argument('-B', '--bailout', action='store_true', help='bail out on non existent paths')
	arg_parser.add_argument('-c', '--create', action='store_true', help='create virtual FS')
	arg_parser.add_argument('-m', '--maxsize', default=None, help='max file size')
	arg_parser.add_argument('-n', '--name', default='', help='virtual file system name')
	arg_parser.add_argument('-p', '--progress', action='store_true', help='view progress')
	arg_parser.add_argument('-b', '--base', default='.', help='base directory, defaults to "."')
	arg_parser.add_argument('-f', '--follow', action='store_true', help='follow directories')
	arg_parser.add_argument('-r', '--relonly', action='store_true', help='force all paths to be relative')
	arg_parser.add_argument('-v', '--verbose', action='store_true', help='increase verbosity')
	arg_parser.add_argument('paths', nargs='*')
	args = arg_parser.parse_args()

	base = os.path.abspath(args.base)

	paths_list = [ ]
	for path in args.paths:
		paths_list.append(os.path.join(base, path))

	if args.from_stdin:
		for path in sys.stdin.readlines():
			paths_list.append(path)

	paths = Paths(base=base, relative_only=args.relonly, paths_list=paths_list)

	fs = FileSystem(base=base, create=args.create, paths=list(paths), follow=args.follow, bailout=args.bailout, maxsize=args.maxsize, progress=args.progress, name=args.name)

	print(fs)

