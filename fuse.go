package hafs

import (
	"errors"
	"io"
	iofs "io/fs"
	"sync"
	"sync/atomic"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

func errToFuseStatus(err error) fuse.Status {
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

	return fuse.Status(fuse.EIO)
}

func fillFuseAttrFromStat(stat *Stat, out *fuse.Attr) {
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

func fillFuseEntryOutFromStat(stat *Stat, out *fuse.EntryOut) {
	out.Generation = 0
	out.NodeId = stat.Ino
	fillFuseAttrFromStat(stat, &out.Attr)
}

type openFile struct {
	ino uint64
	di  *DirIter
}

type FuseFs struct {
	fuse.RawFileSystem
	server *fuse.Server

	fs *Fs

	fileHandleCounter uint64

	lock        sync.Mutex
	fh2OpenFile map[uint64]*openFile
}

func NewFuseFs(fs *Fs) *FuseFs {
	return &FuseFs{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		fs:            fs,
		fh2OpenFile:   make(map[uint64]*openFile),
	}
}

func (fs *FuseFs) nextFileHandle() uint64 {
	return atomic.AddUint64(&fs.fileHandleCounter, 1)
}

func (fs *FuseFs) Init(server *fuse.Server) {
	fs.server = server
}

// Lookup is called by the kernel to refresh an inode in the inode and dent caches.
func (fs *FuseFs) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	stat, err := fs.fs.Lookup(header.NodeId, name)
	if err != nil {
		return errToFuseStatus(err)
	}
	fillFuseEntryOutFromStat(&stat, out)
	return fuse.OK
}

// A forget request is sent by the kernel when it is no longer interested in an inode.
func (fs *FuseFs) Forget(nodeId, nlookup uint64) {
	if nodeId == ^uint64(0) {
		// go-fuse uses this inode for its own purposes (epoll bug fix).
		return
	}
}

func (fs *FuseFs) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	stat, err := fs.fs.GetStat(in.NodeId)
	if err != nil {
		return errToFuseStatus(err)
	}
	fillFuseAttrFromStat(&stat, &out.Attr)
	return fuse.OK
}

func (fs *FuseFs) SetAttr(cancel <-chan struct{}, in *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {

	setStat := SetStatOpts{}

	if mtime, ok := in.GetMTime(); ok {
		setStat.SetMtime(mtime)
	}
	if atime, ok := in.GetATime(); ok {
		setStat.SetAtime(atime)
	}
	if ctime, ok := in.GetCTime(); ok {
		setStat.SetCtime(ctime)
	}

	if size, ok := in.GetSize(); ok {
		setStat.Valid |= SETSTAT_SIZE
		setStat.SetSize(size)
	}

	if mode, ok := in.GetMode(); ok {
		setStat.SetMode(mode)
	}

	if uid, ok := in.GetUID(); ok {
		setStat.SetUid(uid)
	}

	if gid, ok := in.GetGID(); ok {
		setStat.SetGid(gid)
	}

	stat, err := fs.fs.SetStat(in.NodeId, setStat)
	if err != nil {
		return errToFuseStatus(err)
	}

	fillFuseAttrFromStat(&stat, &out.Attr)
	return fuse.OK
}

func (fs *FuseFs) Open(cancel <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	out.Fh = fs.nextFileHandle()
	out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	fs.lock.Lock()
	fs.fh2OpenFile[out.Fh] = &openFile{
		ino: in.NodeId,
	}
	fs.lock.Unlock()
	return fuse.OK
}

func (fs *FuseFs) Create(cancel <-chan struct{}, in *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	stat, err := fs.fs.Mknod(in.NodeId, name, MknodOpts{
		Truncate: in.Flags&unix.O_TRUNC != 0,
		Mode:     (^S_IFMT & in.Mode) | S_IFREG,
		Uid:      in.Owner.Uid,
		Gid:      in.Owner.Gid,
	})
	if err != nil {
		return errToFuseStatus(err)
	}
	fillFuseEntryOutFromStat(&stat, &out.EntryOut)

	out.Fh = fs.nextFileHandle()
	out.OpenFlags |= fuse.FOPEN_DIRECT_IO

	fs.lock.Lock()
	fs.fh2OpenFile[out.Fh] = &openFile{
		ino: stat.Ino,
	}
	fs.lock.Unlock()

	return fuse.OK
}

func (fs *FuseFs) Rename(cancel <-chan struct{}, in *fuse.RenameIn, fromName string, toName string) fuse.Status {
	fromDir := in.NodeId
	toDir := in.Newdir
	err := fs.fs.Rename(fromDir, toDir, fromName, toName)
	if err != nil {
		return errToFuseStatus(err)
	}
	return fuse.OK
}

/*
func (fs *Proto9FS) Read(cancel <-chan struct{}, in *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	n, err := fs.fs.ReadAt(in.NodeId, buf, uint64(in.Offset))
	if err != nil {
		return nil, ErrToStatus(err)
	}
	return fuse.ReadResultData(buf[:n]), fuse.OK
}
*/

func (fs *FuseFs) Write(cancel <-chan struct{}, in *fuse.WriteIn, buf []byte) (uint32, fuse.Status) {
	n, err := fs.fs.WriteData(in.NodeId, buf, uint64(in.Offset))
	if err != nil {
		return n, errToFuseStatus(err)
	}
	return n, fuse.OK
}

/*

func (fs *Proto9FS) setLk(cancel <-chan struct{}, in *fuse.LkIn, wait bool) fuse.Status {

	fs.lock.Lock()
	f := fs.fh2OpenFile[in.Fh]
	fs.lock.Unlock()

	typ9 := uint8(0)

	switch in.Lk.Typ {
	case syscall.F_RDLCK:
		typ9 = proto9.L_LOCK_TYPE_RDLCK
	case syscall.F_WRLCK:
		typ9 = proto9.L_LOCK_TYPE_WRLCK
	case syscall.F_UNLCK:
		typ9 = proto9.L_LOCK_TYPE_UNLCK
	default:
		return fuse.ENOTSUP
	}

	flags9 := uint32(0)
	if wait {
		flags9 |= proto9.L_LOCK_FLAGS_BLOCK
	}

	for {
		status, err := f.f.Lock(proto9.LSetLock{
			Typ:    typ9,
			Flags:  flags9,
			Start:  in.Lk.Start,
			Length: in.Lk.End - in.Lk.Start,
			ProcId: in.Lk.Pid,
		})
		if err != nil {
			return ErrToStatus(err)
		}

		switch status {
		case proto9.L_LOCK_SUCCESS:
			return 0
		case proto9.L_LOCK_BLOCKED:
			if wait {
				// Server doesn't seem to support blocking.
				time.Sleep(1 * time.Second)
				continue
			}
			return fuse.EAGAIN
		default:
			return fuse.EIO
		}
	}
}

func (fs *Proto9FS) SetLk(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return fs.setLk(cancel, in, true)
}

func (fs *Proto9FS) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) fuse.Status {
	return fs.setLk(cancel, in, true)
}

*/

func (fs *FuseFs) Release(cancel <-chan struct{}, in *fuse.ReleaseIn) {
	fs.lock.Lock()
	delete(fs.fh2OpenFile, in.Fh)
	fs.lock.Unlock()
}

func (fs *FuseFs) Unlink(cancel <-chan struct{}, in *fuse.InHeader, name string) fuse.Status {
	err := fs.fs.Unlink(in.NodeId, name)
	return errToFuseStatus(err)
}

func (fs *FuseFs) Rmdir(cancel <-chan struct{}, in *fuse.InHeader, name string) fuse.Status {
	err := fs.fs.Unlink(in.NodeId, name)
	return errToFuseStatus(err)
}

func (fs *FuseFs) Mkdir(cancel <-chan struct{}, in *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	stat, err := fs.fs.Mknod(in.NodeId, name, MknodOpts{
		Mode: (^S_IFMT & in.Mode) | S_IFDIR,
		Uid:  in.Owner.Uid,
		Gid:  in.Owner.Gid,
	})
	if err != nil {
		return errToFuseStatus(err)
	}
	fillFuseEntryOutFromStat(&stat, out)
	return fuse.OK
}

func (fs *FuseFs) OpenDir(cancel <-chan struct{}, in *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	dirIter, err := fs.fs.IterDirEnts(in.NodeId)
	if err != nil {
		return errToFuseStatus(err)
	}

	out.Fh = fs.nextFileHandle()
	out.OpenFlags |= fuse.FOPEN_DIRECT_IO

	fs.lock.Lock()
	fs.fh2OpenFile[out.Fh] = &openFile{
		ino: in.NodeId,
		di:  dirIter,
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
			return errToFuseStatus(err)
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
					return errToFuseStatus(err)
				}
				fillFuseEntryOutFromStat(&stat, entryOut)
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

func (fs *FuseFs) Fsync(cancel <-chan struct{}, in *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

func (fs *FuseFs) FsyncDir(cancel <-chan struct{}, in *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

func (fs *FuseFs) ReleaseDir(in *fuse.ReleaseIn) {
	fs.lock.Lock()
	delete(fs.fh2OpenFile, in.Fh)
	fs.lock.Unlock()
}
