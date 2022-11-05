package hafs

import (
	"errors"
	"io"
	iofs "io/fs"
	"log"
	mathrand "math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

type openFile struct {
	maybeHasPosixLock atomicBool
	di                *DirIter
	f                 HafsFile
}

type HafsFuseOptions struct {
	CacheDentries   time.Duration
	CacheAttributes time.Duration
	Logf            func(string, ...interface{})
}

type FuseFs struct {
	server *fuse.Server

	cacheDentries   time.Duration
	cacheAttributes time.Duration
	logf            func(string, ...interface{})

	fs *Fs

	fileHandleCounter uint64

	lock        sync.Mutex
	fh2OpenFile map[uint64]*openFile
}

func NewFuseFs(fs *Fs, opts HafsFuseOptions) *FuseFs {

	if opts.Logf == nil {
		opts.Logf = log.Printf
	}

	return &FuseFs{
		cacheDentries:   opts.CacheDentries,
		cacheAttributes: opts.CacheAttributes,
		logf:            opts.Logf,
		fs:              fs,
		fh2OpenFile:     make(map[uint64]*openFile),
	}
}

func (fs *FuseFs) errToFuseStatus(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	}

	if errno, ok := err.(unix.Errno); ok {
		return fuse.Status(errno)
	}

	if errors.Is(err, iofs.ErrNotExist) {
		return fuse.Status(unix.ENOENT)
	} else if errors.Is(err, iofs.ErrPermission) {
		return fuse.Status(unix.EPERM)
	} else if errors.Is(err, iofs.ErrExist) {
		return fuse.Status(unix.EEXIST)
	} else if errors.Is(err, iofs.ErrInvalid) {
		return fuse.Status(unix.EINVAL)
	}

	// Log all io errors that don't have a clear cause.
	fs.logf("io error: %s", err)
	return fuse.Status(fuse.EIO)
}

func (fs *FuseFs) fillFuseAttrFromStat(stat *Stat, out *fuse.Attr) {
	out.Ino = stat.Ino
	out.Size = stat.Size
	out.Blocks = stat.Size / 512
	out.Blksize = CHUNK_SIZE
	out.Atime = stat.Atimesec
	out.Atimensec = stat.Atimensec
	out.Mtime = stat.Mtimesec
	out.Mtimensec = stat.Mtimensec
	out.Ctime = stat.Ctimesec
	out.Ctimensec = stat.Ctimensec
	out.Mode = stat.Mode
	out.Nlink = stat.Nlink
	out.Owner.Uid = stat.Uid
	out.Owner.Gid = stat.Gid
	out.Rdev = stat.Rdev
}

func (fs *FuseFs) fillFuseAttrOutFromStat(stat *Stat, out *fuse.AttrOut) {
	fs.fillFuseAttrFromStat(stat, &out.Attr)
	out.AttrValid = uint64(fs.cacheAttributes.Nanoseconds() / 1_000_000_000)
	out.AttrValidNsec = uint32(uint64(fs.cacheAttributes.Nanoseconds()) - out.AttrValid*1_000_000_000)
}

func (fs *FuseFs) fillFuseEntryOutFromStat(stat *Stat, out *fuse.EntryOut) {
	out.Generation = 0
	out.NodeId = stat.Ino
	fs.fillFuseAttrFromStat(stat, &out.Attr)
	out.AttrValid = uint64(fs.cacheAttributes.Nanoseconds() / 1_000_000_000)
	out.AttrValidNsec = uint32(uint64(fs.cacheAttributes.Nanoseconds()) - out.AttrValid*1_000_000_000)

	out.EntryValid = uint64(fs.cacheDentries.Nanoseconds() / 1_000_000_000)
	out.EntryValidNsec = uint32(uint64(fs.cacheDentries.Nanoseconds()) - out.EntryValid*1_000_000_000)

}

func (fs *FuseFs) nextFileHandle() uint64 {
	return atomic.AddUint64(&fs.fileHandleCounter, 1)
}

func (fs *FuseFs) Init(server *fuse.Server) {
	fs.server = server
}

func (fs *FuseFs) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	stat, err := fs.fs.Lookup(header.NodeId, name)
	if err != nil {
		return fs.errToFuseStatus(err)
	}
	fs.fillFuseEntryOutFromStat(&stat, out)
	return fuse.OK
}

func (fs *FuseFs) Forget(nodeId, nlookup uint64) {

}

func (fs *FuseFs) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	stat, err := fs.fs.GetStat(in.NodeId)
	if err != nil {
		return fs.errToFuseStatus(err)
	}
	fs.fillFuseAttrOutFromStat(&stat, out)
	return fuse.OK
}

func (fs *FuseFs) SetAttr(cancel <-chan struct{}, in *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {

	modStat := ModStatOpts{}

	if mtime, ok := in.GetMTime(); ok {
		modStat.SetMtime(mtime)
	}
	if atime, ok := in.GetATime(); ok {
		modStat.SetAtime(atime)
	}
	if ctime, ok := in.GetCTime(); ok {
		modStat.SetCtime(ctime)
	}

	if size, ok := in.GetSize(); ok {
		modStat.Valid |= MODSTAT_SIZE
		modStat.SetSize(size)
	}

	if mode, ok := in.GetMode(); ok {
		modStat.SetMode(mode)
	}

	if uid, ok := in.GetUID(); ok {
		modStat.SetUid(uid)
	}

	if gid, ok := in.GetGID(); ok {
		modStat.SetGid(gid)
	}

	stat, err := fs.fs.ModStat(in.NodeId, modStat)
	if err != nil {
		return fs.errToFuseStatus(err)
	}

	fs.fillFuseAttrOutFromStat(&stat, out)
	return fuse.OK
}

func (fs *FuseFs) Open(cancel <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	f, _, err := fs.fs.OpenFile(in.NodeId, OpenFileOpts{
		Truncate: in.Flags&unix.O_TRUNC != 0,
	})
	if err != nil {
		return fs.errToFuseStatus(err)
	}

	switch f.(type) {
	case *objectStoreReadWriteFile, *objectStoreReadOnlyFile:
	default:
		out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	}

	out.Fh = fs.nextFileHandle()

	fs.lock.Lock()
	fs.fh2OpenFile[out.Fh] = &openFile{
		f: f,
	}
	fs.lock.Unlock()

	return fuse.OK
}

func (fs *FuseFs) Create(cancel <-chan struct{}, in *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	f, stat, err := fs.fs.CreateFile(in.NodeId, name, CreateFileOpts{
		Truncate: in.Flags&unix.O_TRUNC != 0,
		Mode:     in.Mode,
		Uid:      in.Owner.Uid,
		Gid:      in.Owner.Gid,
	})
	if err != nil {
		return fs.errToFuseStatus(err)
	}

	switch f.(type) {
	case *objectStoreReadWriteFile, *objectStoreReadOnlyFile:
	default:
		out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	}

	fs.fillFuseEntryOutFromStat(&stat, &out.EntryOut)

	out.Fh = fs.nextFileHandle()

	fs.lock.Lock()
	fs.fh2OpenFile[out.Fh] = &openFile{f: f}
	fs.lock.Unlock()

	return fuse.OK
}

func (fs *FuseFs) Rename(cancel <-chan struct{}, in *fuse.RenameIn, fromName string, toName string) fuse.Status {
	fromDir := in.NodeId
	toDir := in.Newdir
	err := fs.fs.Rename(fromDir, toDir, fromName, toName)
	if err != nil {
		return fs.errToFuseStatus(err)
	}
	return fuse.OK
}

func (fs *FuseFs) Read(cancel <-chan struct{}, in *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fs.lock.Lock()
	f := fs.fh2OpenFile[in.Fh].f
	fs.lock.Unlock()

	nTotal := uint32(0)
	for nTotal != uint32(len(buf)) {
		n, err := f.ReadData(buf[nTotal:], uint64(in.Offset)+uint64(nTotal))
		nTotal += n
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fs.errToFuseStatus(err)
		}
	}

	return fuse.ReadResultData(buf[:nTotal]), fuse.OK
}

func (fs *FuseFs) Write(cancel <-chan struct{}, in *fuse.WriteIn, buf []byte) (uint32, fuse.Status) {
	fs.lock.Lock()
	f := fs.fh2OpenFile[in.Fh].f
	fs.lock.Unlock()

	nTotal := uint32(0)
	for nTotal != uint32(len(buf)) {
		n, err := f.WriteData(buf[nTotal:], uint64(in.Offset)+uint64(nTotal))
		nTotal += uint32(n)
		if err != nil {
			return nTotal, fs.errToFuseStatus(err)
		}
	}

	return nTotal, fuse.OK
}

func (fs *FuseFs) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	// XXX We do support sparse files, so this could be implemented.
	// It's worth noting that it seems like fuse only uses Lseek for SEEK_DATA and SEEK_HOLE but
	// we could be wrong on that.
	return fuse.ENOSYS
}

func (fs *FuseFs) Fsync(cancel <-chan struct{}, in *fuse.FsyncIn) fuse.Status {
	fs.lock.Lock()
	f := fs.fh2OpenFile[in.Fh].f
	fs.lock.Unlock()

	err := f.Fsync()
	if err != nil {
		return fs.errToFuseStatus(err)
	}
	// XXX are we supposed to release locks here or in release.
	return fuse.OK
}

func (fs *FuseFs) Flush(cancel <-chan struct{}, in *fuse.FlushIn) fuse.Status {
	fs.lock.Lock()
	f := fs.fh2OpenFile[in.Fh]
	fs.lock.Unlock()

	fsyncErr := f.f.Fsync()

	if f.maybeHasPosixLock.Load() {
		// Note, this this behavior intentionally violates posix lock semantics:
		//
		// Normally posix locks are associated with a process id, so any file descriptor
		// that opens and closes a file will release the lock for all files, but here we only
		// are only releasing it if the file that created the lock is closed.
		//
		// We *could* implement the full semantics, but at increased complexity, reduced
		// performance, and mainly to support potentially questionable use cases. For now
		// we will instead document our semantics and keep it simple.
		f.maybeHasPosixLock.Store(false)
		fs.releaseLocks(in.NodeId, in.LockOwner)
	}
	return fs.errToFuseStatus(fsyncErr)
}

func (fs *FuseFs) Release(cancel <-chan struct{}, in *fuse.ReleaseIn) {
	fs.lock.Lock()
	f := fs.fh2OpenFile[in.Fh]
	delete(fs.fh2OpenFile, in.Fh)
	fs.lock.Unlock()

	_ = f.f.Close()

	const FUSE_RELEASE_FLOCK_UNLOCK = (1 << 1) // XXX remove once constant is in upstream go-fuse.
	if in.ReleaseFlags&FUSE_RELEASE_FLOCK_UNLOCK != 0 {
		fs.releaseLocks(in.NodeId, in.LockOwner)
	}
}

func (fs *FuseFs) Unlink(cancel <-chan struct{}, in *fuse.InHeader, name string) fuse.Status {
	err := fs.fs.Unlink(in.NodeId, name)
	return fs.errToFuseStatus(err)
}

func (fs *FuseFs) Rmdir(cancel <-chan struct{}, in *fuse.InHeader, name string) fuse.Status {
	err := fs.fs.Unlink(in.NodeId, name)
	return fs.errToFuseStatus(err)
}

func (fs *FuseFs) Link(cancel <-chan struct{}, in *fuse.LinkIn, name string, out *fuse.EntryOut) fuse.Status {
	stat, err := fs.fs.HardLink(in.NodeId, in.Oldnodeid, name)
	if err != nil {
		return fs.errToFuseStatus(err)
	}
	fs.fillFuseEntryOutFromStat(&stat, out)
	return fuse.OK
}

func (fs *FuseFs) Symlink(cancel <-chan struct{}, in *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) fuse.Status {
	stat, err := fs.fs.Mknod(in.NodeId, linkName, MknodOpts{
		Mode:       S_IFLNK | 0o777,
		Uid:        in.Owner.Uid,
		Gid:        in.Owner.Gid,
		LinkTarget: []byte(pointedTo),
	})
	if err != nil {
		return fs.errToFuseStatus(err)
	}
	fs.fillFuseEntryOutFromStat(&stat, out)
	return fuse.OK
}

func (fs *FuseFs) Readlink(cancel <-chan struct{}, in *fuse.InHeader) ([]byte, fuse.Status) {
	l, err := fs.fs.ReadSymlink(in.NodeId)
	if err != nil {
		return nil, fs.errToFuseStatus(err)
	}
	return l, fuse.OK
}

func (fs *FuseFs) Mkdir(cancel <-chan struct{}, in *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	stat, err := fs.fs.Mknod(in.NodeId, name, MknodOpts{
		Mode: (^S_IFMT & in.Mode) | S_IFDIR,
		Uid:  in.Owner.Uid,
		Gid:  in.Owner.Gid,
	})
	if err != nil {
		return fs.errToFuseStatus(err)
	}
	fs.fillFuseEntryOutFromStat(&stat, out)
	return fuse.OK
}

func (fs *FuseFs) OpenDir(cancel <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	dirIter, err := fs.fs.IterDirEnts(in.NodeId)
	if err != nil {
		return fs.errToFuseStatus(err)
	}

	out.Fh = fs.nextFileHandle()
	out.OpenFlags |= fuse.FOPEN_DIRECT_IO

	fs.lock.Lock()
	fs.fh2OpenFile[out.Fh] = &openFile{
		di: dirIter,
		f:  &invalidFile{},
	}
	fs.lock.Unlock()

	return fuse.OK
}

func (fs *FuseFs) readDir(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList, plus bool) fuse.Status {
	fs.lock.Lock()
	d := fs.fh2OpenFile[in.Fh]
	fs.lock.Unlock()

	if d.di == nil {
		return fuse.Status(unix.EBADF)
	}

	// XXX TODO verify offset is correct.
	for {
		ent, err := d.di.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fs.errToFuseStatus(err)
		}
		fuseDirEnt := fuse.DirEntry{
			Name: ent.Name,
			Mode: ent.Mode,
			Ino:  ent.Ino,
		}
		if plus {
			// XXX avoid multiple stats with DirPlusIter ?
			entryOut := out.AddDirLookupEntry(fuseDirEnt)
			if entryOut != nil {
				stat, err := fs.fs.GetStat(ent.Ino)
				if err != nil {
					return fs.errToFuseStatus(err)
				}
				fs.fillFuseEntryOutFromStat(&stat, entryOut)
			} else {
				d.di.Unget(ent)
				break
			}
		} else {
			if !out.AddDirEntry(fuseDirEnt) {
				d.di.Unget(ent)
				break
			}
		}
	}
	return fuse.OK
}

func (fs *FuseFs) ReadDir(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fs.readDir(cancel, in, out, false)
}

func (fs *FuseFs) ReadDirPlus(cancel <-chan struct{}, in *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	return fs.readDir(cancel, in, out, true)
}

func (fs *FuseFs) FsyncDir(cancel <-chan struct{}, in *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

func (fs *FuseFs) ReleaseDir(in *fuse.ReleaseIn) {
	fs.lock.Lock()
	delete(fs.fh2OpenFile, in.Fh)
	fs.lock.Unlock()
}

func (fs *FuseFs) GetXAttr(cancel <-chan struct{}, in *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	x, err := fs.fs.GetXAttr(in.NodeId, attr)
	if err != nil {
		return 0, fs.errToFuseStatus(err)
	}
	if len(dest) < len(x) {
		return uint32(len(x)), fuse.ERANGE
	}
	copy(dest, x)
	return uint32(len(x)), fuse.OK
}

func (fs *FuseFs) ListXAttr(cancel <-chan struct{}, in *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	xattrs, err := fs.fs.ListXAttr(in.NodeId)
	if err != nil {
		return 0, fs.errToFuseStatus(err)
	}

	nNeeded := uint32(0)
	for _, x := range xattrs {
		nNeeded += uint32(len(x)) + 1
	}
	if uint32(len(dest)) < nNeeded {
		return nNeeded, fuse.ERANGE
	}

	for _, x := range xattrs {
		copy(dest[:len(x)], x)
		dest[len(x)] = 0
		dest = dest[len(x)+1:]
	}

	return nNeeded, fuse.OK
}

func (fs *FuseFs) SetXAttr(cancel <-chan struct{}, in *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	err := fs.fs.SetXAttr(in.NodeId, attr, data)
	return fs.errToFuseStatus(err)
}

func (fs *FuseFs) RemoveXAttr(cancel <-chan struct{}, in *fuse.InHeader, attr string) fuse.Status {
	err := fs.fs.RemoveXAttr(in.NodeId, attr)
	return fs.errToFuseStatus(err)
}

func (fs *FuseFs) releaseLocks(ino uint64, lockOwner uint64) {
	for {
		_, err := fs.fs.TrySetLock(ino, SetLockOpts{
			Typ:   LOCK_NONE,
			Owner: lockOwner,
		})
		if err == nil {
			break
		}
		fs.logf("unable to release lock ino=%d owner=%d: %s", ino, lockOwner, err)
		time.Sleep(1 * time.Second)
	}
}

func (fs *FuseFs) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FuseFs) SetLk(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	fs.lock.Lock()
	f := fs.fh2OpenFile[in.Fh]
	fs.lock.Unlock()

	var lockType LockType

	switch in.Lk.Typ {
	case unix.F_RDLCK:
		lockType = LOCK_SHARED
	case unix.F_WRLCK:
		lockType = LOCK_EXCLUSIVE
	case unix.F_UNLCK:
		lockType = LOCK_NONE
	default:
		return fuse.ENOTSUP
	}

	if in.Lk.Start != 0 {
		return fuse.ENOTSUP
	}
	if in.Lk.End != 0x7fffffffffffffff {
		return fuse.ENOTSUP
	}

	ok, err := fs.fs.TrySetLock(in.NodeId, SetLockOpts{
		Typ:   lockType,
		Owner: in.Owner,
	})

	if in.LkFlags&fuse.FUSE_LK_FLOCK == 0 {
		f.maybeHasPosixLock.Store(true)
	}

	if err != nil {
		return fs.errToFuseStatus(err)
	}

	if !ok {
		return fuse.EAGAIN
	}

	return fuse.OK
}

func (fs *FuseFs) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	nAttempts := uint64(0)
	for {
		status := fs.SetLk(cancel, in)
		if status != fuse.EAGAIN {
			return status
		}

		select {
		case <-cancel:
			return fuse.EINTR
		default:
		}

		if nAttempts >= 2 {
			// Random delay to partially mitigate thundering herd on contended lock.
			time.Sleep(time.Duration(mathrand.Int()%5_000) * time.Millisecond)
		}
		err := fs.fs.AwaitExclusiveLockRelease(cancel, in.NodeId)
		if err != nil {
			return fs.errToFuseStatus(err)
		}
		nAttempts += 1
	}
}

func (fs *FuseFs) StatFs(cancel <-chan struct{}, in *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {

	stats, err := fs.fs.FsStats()
	if err != nil {
		return fs.errToFuseStatus(err)
	}

	out.Bsize = CHUNK_SIZE
	out.Blocks = stats.UsedBytes / CHUNK_SIZE
	out.Bfree = stats.FreeBytes / CHUNK_SIZE
	out.Bavail = out.Bfree
	out.NameLen = 4096

	return fuse.OK
}

func (fs *FuseFs) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FuseFs) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FuseFs) Fallocate(cancel <-chan struct{}, in *fuse.FallocateIn) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FuseFs) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FuseFs) SetDebug(dbg bool) {
}

func (fs *FuseFs) String() string {
	return os.Args[0]
}
