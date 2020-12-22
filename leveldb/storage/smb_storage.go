package storage

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hirochachacha/go-smb2"
)

type smbFileLock struct {
	f    *smb2.File
	name string
}

func (lock *smbFileLock) release() {
	lock.f.Close()
	return
}

type smbFileStorageLock struct {
	fs *smbFileStorage
}

func (lock *smbFileStorageLock) Unlock() {
	if lock.fs != nil {
		lock.fs.mu.Lock()
		defer lock.fs.mu.Unlock()
		if lock.fs.slock == lock {
			lock.fs.slock = nil
		}
	}
}

// fileStorage is a file-system backed storage.
type smbFileStorage struct {
	path     string
	filename string
	readOnly bool

	conn    net.Conn
	sess    *smb2.Session
	smb     *smb2.Share
	mu      sync.Mutex
	flock   smbFileLock
	slock   *smbFileStorageLock
	logw    *smb2.File
	logSize int64
	buf     []byte
	// Opened file counter; if open < 0 means closed.
	open int
	day  int
	meta FileDesc
}

// OpenSmbFile returns a new smb filesystem-backed storage implementation with the given
// path. This also acquire a file lock, so any subsequent attempt to open the
// same path will fail.
//
// The smb share storage must be closed after use, by calling Close method.
func OpenSmbFile(path string, hostname string, share string, user string, passwd string, readOnly bool) (Storage, error) {

	fmt.Printf("Open file %s on SMB share \\\\%s\\%s \n", path, hostname, share)

	conn, err := net.Dial("tcp", hostname)
	if err != nil {
		fmt.Printf("net Dial returning error\n")
		return nil, err
	}

	d := &smb2.Dialer{
		Initiator: &smb2.NTLMInitiator{
			User:     user,
			Password: passwd,
		},
	}

	s, err := d.Dial(conn)
	if err != nil {
		fmt.Printf("Dial returning error\n")
		return nil, err
	}

	smb, err := s.Mount(share)
	if err != nil {
		fmt.Printf("Mount returning error\n")
		return nil, err
	}

	if fi, err := smb.Stat(path); err == nil {
		if !fi.IsDir() {
			return nil, fmt.Errorf("leveldb/storage: open %s: not a directory", path)
		}
	} else {
		err := smb.MkdirAll(path, 0755)
		if err != nil {
			fmt.Printf("MkdirAll %s returning error\n", path)
			return nil, err
		}
	}

	fs := &smbFileStorage{
		smb:      smb,
		path:     path,
		readOnly: readOnly,
	}

	fs.flock, err = fs.NewFileLock(filepath.Join(path, "LOCK"), readOnly)
	if err != nil {
		return nil, err
	}

	if !readOnly {
		fs.logw, err = smb.OpenFile(filepath.Join(path, "LOG"), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			fmt.Printf("OpenFile for Log returning error\n")
			return nil, err
		}
		fs.logSize, err = fs.logw.Seek(0, os.SEEK_END)
		if err != nil {
			fs.logw.Close()
			fmt.Printf("logw.Seek for Log returning error\n")
			return nil, err
		}
	}

	runtime.SetFinalizer(fs, (*smbFileStorage).Close)

	return fs, nil
}

func (fs *smbFileStorage) NewFileLock(path string, readOnly bool) (fl smbFileLock, err error) {
	fmt.Println("In New File Lock")

	f, err := fs.smb.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		f, err = fs.smb.Create(path)
		if err != nil {
			fmt.Println("Open File %s , no error. Another process is running", path)
			err = os.ErrClosed
			return
		}
	} else {
		fmt.Println("Open File %s , no error. Another process is running", path)
		err = os.ErrClosed
		return
	}

	fl = smbFileLock{f: f, name: path}
	return
}

func (fs *smbFileStorage) Lock() (Locker, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	if fs.readOnly {
		return &smbFileStorageLock{}, nil
	}
	if fs.slock != nil {
		return nil, ErrLocked
	}
	fs.slock = &smbFileStorageLock{fs: fs}
	return fs.slock, nil
}

func (fs *smbFileStorage) printDay(t time.Time) {
	if fs.day == t.Day() {
		return
	}
	fs.day = t.Day()
	fs.logw.Write([]byte("=============== " + t.Format("Jan 2, 2006 (MST)") + " ===============\n"))
}

func (fs *smbFileStorage) doLog(t time.Time, str string) {
	if fs.logSize > logSizeThreshold {
		// Rotate log file.
		fs.logw.Close()
		fs.logw = nil
		fs.logSize = 0
		fs.smb.Rename(filepath.Join(fs.path, "LOG"), filepath.Join(fs.path, "LOG.old"))
	}
	if fs.logw == nil {
		var err error
		fs.logw, err = fs.smb.OpenFile(filepath.Join(fs.path, "LOG"), os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return
		}
		// Force printDay on new log file.
		fs.day = 0
	}
	fs.printDay(t)
	hour, min, sec := t.Clock()
	msec := t.Nanosecond() / 1e3
	// time
	fs.buf = itoa(fs.buf[:0], hour, 2)
	fs.buf = append(fs.buf, ':')
	fs.buf = itoa(fs.buf, min, 2)
	fs.buf = append(fs.buf, ':')
	fs.buf = itoa(fs.buf, sec, 2)
	fs.buf = append(fs.buf, '.')
	fs.buf = itoa(fs.buf, msec, 6)
	fs.buf = append(fs.buf, ' ')
	// write
	fs.buf = append(fs.buf, []byte(str)...)
	fs.buf = append(fs.buf, '\n')
	n, _ := fs.logw.Write(fs.buf)
	fs.logSize += int64(n)
}

func (fs *smbFileStorage) Log(str string) {
	if !fs.readOnly {
		t := time.Now()
		fs.mu.Lock()
		defer fs.mu.Unlock()
		if fs.open < 0 {
			return
		}
		fs.doLog(t, str)
	}
}

func (fs *smbFileStorage) log(str string) {
	if !fs.readOnly {
		fs.doLog(time.Now(), str)
	}
}

func (fs *smbFileStorage) smbWriteFileSynced(filename string, data []byte, perm os.FileMode) error {

	f, err := fs.smb.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err1 := f.Sync(); err == nil {
		err = err1
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

func (fs *smbFileStorage) setMeta(fd FileDesc) error {
	content := fsGenName(fd) + "\n"
	// Check and backup old CURRENT file.
	currentPath := filepath.Join(fs.path, "CURRENT")
	if _, err := fs.smb.Stat(currentPath); err == nil {
		b, err := fs.smb.ReadFile(currentPath)
		if err != nil {
			fs.log(fmt.Sprintf("backup CURRENT: %v", err))
			return err
		}
		if string(b) == content {
			// Content not changed, do nothing.
			return nil
		}
		if err := fs.smbWriteFileSynced(currentPath+".bak", b, 0644); err != nil {
			fs.log(fmt.Sprintf("backup CURRENT: %v", err))
			return err
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	path := fmt.Sprintf("%s.%d", filepath.Join(fs.path, "CURRENT"), fd.Num)
	if err := fs.smbWriteFileSynced(path, []byte(content), 0644); err != nil {
		fs.log(fmt.Sprintf("create CURRENT.%d: %v", fd.Num, err))
		return err
	}

	fs.log(fmt.Sprintf("created CURRENT.%d successfully", fd.Num))

	// Replace CURRENT file.
	fs.smb.Remove(currentPath)
	if err := fs.smb.Rename(path, currentPath); err != nil {
		fs.log(fmt.Sprintf("rename CURRENT.%d: %v", fd.Num, err))
		return err
	}

	// Sync root directory.
	if err := fs.syncDir(fs.path); err != nil {
		fs.log(fmt.Sprintf("syncDir: %v", err))
		return err
	}

	return nil
}

func (fs *smbFileStorage) SetMeta(fd FileDesc) error {
	if !FileDescOk(fd) {
		return ErrInvalidFile
	}
	if fs.readOnly {
		return errReadOnly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}
	return fs.setMeta(fd)
}

func (fs *smbFileStorage) GetMeta() (FileDesc, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return FileDesc{}, ErrClosed
	}
	dir, err := fs.smb.Open(fs.path)
	if err != nil {
		return FileDesc{}, err
	}
	names, err := dir.Readdirnames(0)
	// Close the dir first before checking for Readdirnames error.
	if ce := dir.Close(); ce != nil {
		fs.log(fmt.Sprintf("close dir: %v", ce))
	}
	if err != nil {
		return FileDesc{}, err
	}
	// Try this in order:
	// - CURRENT.[0-9]+ ('pending rename' file, descending order)
	// - CURRENT
	// - CURRENT.bak
	//
	// Skip corrupted file or file that point to a missing target file.
	type currentFile struct {
		name string
		fd   FileDesc
	}
	tryCurrent := func(name string) (*currentFile, error) {
		b, err := fs.smb.ReadFile(filepath.Join(fs.path, name))
		if err != nil {
			if os.IsNotExist(err) {
				err = os.ErrNotExist
			}
			return nil, err
		}
		var fd FileDesc
		if len(b) < 1 || b[len(b)-1] != '\n' || !fsParseNamePtr(string(b[:len(b)-1]), &fd) {
			fs.log(fmt.Sprintf("%s: corrupted content: %q", name, b))
			err := &ErrCorrupted{
				Err: errors.New("leveldb/storage: corrupted or incomplete CURRENT file"),
			}
			return nil, err
		}
		if _, err := fs.smb.Stat(filepath.Join(fs.path, fsGenName(fd))); err != nil {
			if os.IsNotExist(err) {
				fs.log(fmt.Sprintf("%s: missing target file: %s", name, fd))
				err = os.ErrNotExist
			}
			return nil, err
		}
		return &currentFile{name: name, fd: fd}, nil
	}
	tryCurrents := func(names []string) (*currentFile, error) {
		var (
			cur *currentFile
			// Last corruption error.
			lastCerr error
		)
		for _, name := range names {
			var err error
			cur, err = tryCurrent(name)
			if err == nil {
				break
			} else if err == os.ErrNotExist {
				// Fallback to the next file.
			} else if isCorrupted(err) {
				lastCerr = err
				// Fallback to the next file.
			} else {
				// In case the error is due to permission, etc.
				return nil, err
			}
		}
		if cur == nil {
			err := os.ErrNotExist
			if lastCerr != nil {
				err = lastCerr
			}
			return nil, err
		}
		return cur, nil
	}

	// Try 'pending rename' files.
	var nums []int64
	for _, name := range names {
		if strings.HasPrefix(name, "CURRENT.") && name != "CURRENT.bak" {
			i, err := strconv.ParseInt(name[8:], 10, 64)
			if err == nil {
				nums = append(nums, i)
			}
		}
	}
	var (
		pendCur   *currentFile
		pendErr   = os.ErrNotExist
		pendNames []string
	)
	if len(nums) > 0 {
		sort.Sort(sort.Reverse(int64Slice(nums)))
		pendNames = make([]string, len(nums))
		for i, num := range nums {
			pendNames[i] = fmt.Sprintf("CURRENT.%d", num)
		}
		pendCur, pendErr = tryCurrents(pendNames)
		if pendErr != nil && pendErr != os.ErrNotExist && !isCorrupted(pendErr) {
			return FileDesc{}, pendErr
		}
	}

	// Try CURRENT and CURRENT.bak.
	curCur, curErr := tryCurrents([]string{"CURRENT", "CURRENT.bak"})
	if curErr != nil && curErr != os.ErrNotExist && !isCorrupted(curErr) {
		return FileDesc{}, curErr
	}

	// pendCur takes precedence, but guards against obsolete pendCur.
	if pendCur != nil && (curCur == nil || pendCur.fd.Num > curCur.fd.Num) {
		curCur = pendCur
	}

	if curCur != nil {
		// Restore CURRENT file to proper state.
		if !fs.readOnly && (curCur.name != "CURRENT" || len(pendNames) != 0) {
			// Ignore setMeta errors, however don't delete obsolete files if we
			// catch error.
			if err := fs.setMeta(curCur.fd); err == nil {
				// Remove 'pending rename' files.
				for _, name := range pendNames {
					fs.smb.Remove(filepath.Join(fs.path, name))
				}
			}
		}
		return curCur.fd, nil
	}

	// Nothing found.
	if isCorrupted(pendErr) {
		return FileDesc{}, pendErr
	}
	return FileDesc{}, curErr
}

func (fs *smbFileStorage) List(ft FileType) (fds []FileDesc, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	dir, err := fs.smb.Open(fs.path)
	if err != nil {
		return
	}
	names, err := dir.Readdirnames(0)
	// Close the dir first before checking for Readdirnames error.
	if cerr := dir.Close(); cerr != nil {
		fs.log(fmt.Sprintf("close dir: %v", cerr))
	}
	if err == nil {
		for _, name := range names {
			if fd, ok := fsParseName(name); ok && fd.Type&ft != 0 {
				fds = append(fds, fd)
			}
		}
	}
	return
}

func (fs *smbFileStorage) Open(fd FileDesc) (Reader, error) {
	if !FileDescOk(fd) {
		return nil, ErrInvalidFile
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	of, err := fs.smb.OpenFile(filepath.Join(fs.path, fsGenName(fd)), os.O_RDONLY, 0)
	if err != nil {
		if fsHasOldName(fd) {
			of, err = fs.smb.OpenFile(filepath.Join(fs.path, fsGenOldName(fd)), os.O_RDONLY, 0)
			if err == nil {
				goto ok
			}
		}
		return nil, err
	}
ok:
	fs.open++
	return &smbFileWrap{File: of, fs: fs, fd: fd}, nil
}

func (fs *smbFileStorage) Create(fd FileDesc) (Writer, error) {
	if !FileDescOk(fd) {
		return nil, ErrInvalidFile
	}
	if fs.readOnly {
		return nil, errReadOnly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return nil, ErrClosed
	}
	of, err := fs.smb.OpenFile(filepath.Join(fs.path, fsGenName(fd)), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	fs.open++
	return &smbFileWrap{File: of, fs: fs, fd: fd}, nil
}

func (fs *smbFileStorage) Remove(fd FileDesc) error {
	if !FileDescOk(fd) {
		return ErrInvalidFile
	}
	if fs.readOnly {
		return errReadOnly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}
	err := fs.smb.Remove(filepath.Join(fs.path, fsGenName(fd)))
	if err != nil && os.IsNotExist(err) {
		if fsHasOldName(fd) {
			if e1 := fs.smb.Remove(filepath.Join(fs.path, fsGenOldName(fd))); !os.IsNotExist(e1) {
				fs.log(fmt.Sprintf("smbFileStorage::Remove remove %s: %v (old name)", fd, err))
				err = e1
			}
		} else {
			fs.log(fmt.Sprintf("smbFileStorage::Remove remove %s: %v", fd, err))
		}
	}
	return err
}

func (fs *smbFileStorage) Rename(oldfd, newfd FileDesc) error {
	if !FileDescOk(oldfd) || !FileDescOk(newfd) {
		return ErrInvalidFile
	}
	if oldfd == newfd {
		return nil
	}
	if fs.readOnly {
		return errReadOnly
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}
	return fs.smb.Rename(filepath.Join(fs.path, fsGenName(oldfd)), filepath.Join(fs.path, fsGenName(newfd)))
}

func (fs *smbFileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.open < 0 {
		return ErrClosed
	}
	// Clear the finalizer.
	runtime.SetFinalizer(fs, nil)

	if fs.open > 0 {
		fs.log(fmt.Sprintf("close: warning, %d files still open", fs.open))
	}
	fs.open = -1
	if fs.logw != nil {
		fs.logw.Close()
	}

	//Close the lock file
	fs.flock.release()
	fs.smb.Remove(fs.flock.name)

	//Close the smb session
	fs.smb.Umount()
	return nil
}

func (fs *smbFileStorage) syncDir(name string) error {
	f, err := fs.smb.Open(name)
	if err != nil {
		return err
	}

	defer f.Close()
	err = f.Sync()

	return nil
}

type smbFileWrap struct {
	*smb2.File
	fs     *smbFileStorage
	fd     FileDesc
	closed bool
}

func (fw *smbFileWrap) Sync() error {
	if err := fw.File.Sync(); err != nil {
		return err
	}
	if fw.fd.Type == TypeManifest {
		fw.File.Sync()
	}
	return nil
}

func (fw *smbFileWrap) Close() error {
	fw.fs.mu.Lock()
	defer fw.fs.mu.Unlock()
	if fw.closed {
		return ErrClosed
	}
	fw.closed = true
	fw.fs.open--
	err := fw.File.Close()
	if err != nil {
		fw.fs.log(fmt.Sprintf("close %s: %v", fw.fd, err))
	}
	return err
}
