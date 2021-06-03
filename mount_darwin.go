package fuse

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"
)

var (
	errNoAvail   = errors.New("no available fuse devices")
	errNotLoaded = errors.New("osxfuse is not loaded")
)

func loadOSXFUSE(bin string) error {
	cmd := exec.Command(bin)
	cmd.Dir = "/"
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	return err
}

func openOSXFUSEDev(devPrefix string) (*os.File, error) {
	var f *os.File
	var err error
	for i := uint64(0); ; i++ {
		path := devPrefix + strconv.FormatUint(i, 10)
		f, err = os.OpenFile(path, os.O_RDWR, 0000)
		if os.IsNotExist(err) {
			if i == 0 {
				// not even the first device was found -> fuse is not loaded
				return nil, errNotLoaded
			}

			// we've run out of kernel-provided devices
			return nil, errNoAvail
		}

		if err2, ok := err.(*os.PathError); ok && err2.Err == syscall.EBUSY {
			// try the next one
			continue
		}

		if err != nil {
			return nil, err
		}
		return f, nil
	}
}

func handleMountOSXFUSE(helperName string, errCh chan<- error) func(line string) (ignore bool) {
	var noMountpointPrefix = helperName + `: `
	const noMountpointSuffix = `: No such file or directory`
	return func(line string) (ignore bool) {
		if strings.HasPrefix(line, noMountpointPrefix) && strings.HasSuffix(line, noMountpointSuffix) {
			// re-extract it from the error message in case some layer
			// changed the path
			mountpoint := line[len(noMountpointPrefix) : len(line)-len(noMountpointSuffix)]
			err := &MountpointDoesNotExistError{
				Path: mountpoint,
			}
			select {
			case errCh <- err:
				return true
			default:
				// not the first error; fall back to logging it
				return false
			}
		}

		return false
	}
}

// isBoringMountOSXFUSEError returns whether the Wait error is
// uninteresting; exit status 64 is.
func isBoringMountOSXFUSEError(err error) bool {
	if err, ok := err.(*exec.ExitError); ok && err.Exited() {
		if status, ok := err.Sys().(syscall.WaitStatus); ok && status.ExitStatus() == 64 {
			return true
		}
	}
	return false
}

func callMount(bin string, daemonVar string, dir string, conf *mountConfig, f *os.File, ready chan<- struct{}, errp *error) error {
	for k, v := range conf.options {
		if strings.Contains(k, ",") || strings.Contains(v, ",") {
			// Silly limitation but the mount helper does not
			// understand any escaping. See TestMountOptionCommaError.
			return fmt.Errorf("mount options cannot contain commas on darwin: %q=%q", k, v)
		}
	}
	cmd := exec.Command(
		bin,
		"-o", conf.getOptions(),
		// Tell osxfuse-kext how large our buffer is. It must split
		// writes larger than this into multiple writes.
		//
		// OSXFUSE seems to ignore InitResponse.MaxWrite, and uses
		// this instead.
		"-o", "iosize="+strconv.FormatUint(maxWrite, 10),
		// refers to fd passed in cmd.ExtraFiles
		"3",
		dir,
	)
	cmd.ExtraFiles = []*os.File{f}
	cmd.Env = os.Environ()
	// OSXFUSE <3.3.0
	cmd.Env = append(cmd.Env, "MOUNT_FUSEFS_CALL_BY_LIB=")
	// OSXFUSE >=3.3.0
	cmd.Env = append(cmd.Env, "MOUNT_OSXFUSE_CALL_BY_LIB=")

	daemon := os.Args[0]
	if daemonVar != "" {
		cmd.Env = append(cmd.Env, daemonVar+"="+daemon)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("setting up mount_osxfusefs stderr: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("setting up mount_osxfusefs stderr: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("mount_osxfusefs: %v", err)
	}
	helperErrCh := make(chan error, 1)
	go func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go lineLogger(&wg, "mount helper output", neverIgnoreLine, stdout)
		helperName := path.Base(bin)
		go lineLogger(&wg, "mount helper error", handleMountOSXFUSE(helperName, helperErrCh), stderr)
		wg.Wait()
		if err := cmd.Wait(); err != nil {
			// see if we have a better error to report
			select {
			case helperErr := <-helperErrCh:
				// log the Wait error if it's not what we expected
				if !isBoringMountOSXFUSEError(err) {
					log.Printf("mount helper failed: %v", err)
				}
				// and now return what we grabbed from stderr as the real
				// error
				*errp = helperErr
				close(ready)
				return
			default:
				// nope, fall back to generic message
			}

			*errp = fmt.Errorf("mount_osxfusefs: %v", err)
			close(ready)
			return
		}

		*errp = nil
		close(ready)
	}()
	return nil
}

func mount(dir string, conf *mountConfig, ready chan<- struct{}, errp *error) (*os.File, error) {
	locations := conf.osxfuseLocations
	if locations == nil {
		locations = []OSXFUSEPaths{
			OSXFUSELocationV3,
			OSXFUSELocationV2,
		}
	}
	for _, loc := range locations {
		if _, err := os.Stat(loc.Mount); os.IsNotExist(err) {
			// try the other locations
			continue
		}

		f, err := openOSXFUSEDev(loc.DevicePrefix)
		if err == errNotLoaded {
			err = loadOSXFUSE(loc.Load)
			if err != nil {
				return nil, err
			}
			// try again
			f, err = openOSXFUSEDev(loc.DevicePrefix)
		}
		if err != nil {
			return nil, err
		}
		err = callMount(loc.Mount, loc.DaemonVar, dir, conf, f, ready, errp)
		if err != nil {
			f.Close()
			return nil, err
		}
		return f, nil
	}

	// macfuse v4
	locations = []OSXFUSEPaths{
		OSXFUSELocationV4,
	}
	for _, loc := range locations {
		if _, err := os.Stat(loc.Mount); os.IsNotExist(err) {
			// try the other locations
			continue
		}

		local, remote, err := unixgramSocketpair()
		if err != nil {
			return nil, fmt.Errorf("create socket pair: %v", err)
		}

		defer local.Close()
		defer remote.Close()

		cmd := exec.Command(loc.Mount,
			"-o", conf.getOptions(),
			"-o", "iosize="+strconv.FormatUint(maxWrite, 10),
			dir)
		cmd.ExtraFiles = []*os.File{remote} // fd would be (index + 3)
		cmd.Env = append(os.Environ(),
			"_FUSE_CALL_BY_LIB=",
			"_FUSE_DAEMON_PATH="+os.Args[0],
			"_FUSE_COMMFD=3",
			"_FUSE_COMMVERS=2",
			"MOUNT_OSXFUSE_CALL_BY_LIB=",
			"MOUNT_OSXFUSE_DAEMON_PATH="+os.Args[0])

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, fmt.Errorf("setting up mount_osxfusefs stderr: %v", err)
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			return nil, fmt.Errorf("setting up mount_osxfusefs stderr: %v", err)
		}

		if err = cmd.Start(); err != nil {
			return nil, fmt.Errorf("cmd start: %v", err)
		}

		fd, err := getConnection(local)
		if err != nil {
			return nil, err
		}
		syscall.CloseOnExec(fd)

		helperErrCh := make(chan error, 1)
		go func() {
			var wg sync.WaitGroup
			wg.Add(2)
			go lineLogger(&wg, "mount helper output", neverIgnoreLine, stdout)
			helperName := path.Base(loc.Mount)
			go lineLogger(&wg, "mount helper error", handleMountOSXFUSE(helperName, helperErrCh), stderr)
			wg.Wait()
			// wait inside a goroutine or otherwise it would block forever for unknown reasons
			if err := cmd.Wait(); err != nil {
				// see if we have a better error to report
				select {
				case helperErr := <-helperErrCh:
					// log the Wait error if it's not what we expected
					if !isBoringMountOSXFUSEError(err) {
						log.Printf("mount helper failed: %v", err)
					}
					// and now return what we grabbed from stderr as the real
					// error
					*errp = helperErr
					close(ready)
					return
				default:
					// nope, fall back to generic message
				}
				*errp = fmt.Errorf("mount_osxfusefs: %v", err)
				close(ready)
				return
			}
			*errp = nil
			close(ready)

			return
		}()

		return os.NewFile(uintptr(fd), "conn"), nil
	}

	return nil, ErrOSXFUSENotFound
}

// based on https://github.com/hanwen/go-fuse/commit/09a3c381714cf1011fb2d08885f29896cd496a0c
func unixgramSocketpair() (l, r *os.File, err error) {
	fd, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, os.NewSyscallError("socketpair",
			err.(syscall.Errno))
	}
	l = os.NewFile(uintptr(fd[0]), "socketpair-half1")
	r = os.NewFile(uintptr(fd[1]), "socketpair-half2")
	return
}

func getConnection(local *os.File) (int, error) {
	var data [4]byte
	control := make([]byte, 4*256)

	// n, oobn, recvflags, from, errno  - todo: error checking.
	_, oobn, _, _,
	err := syscall.Recvmsg(
		int(local.Fd()), data[:], control[:], 0)
	if err != nil {
		return 0, err
	}

	message := *(*syscall.Cmsghdr)(unsafe.Pointer(&control[0]))
	fd := *(*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(&control[0])) + syscall.SizeofCmsghdr))

	if message.Type != syscall.SCM_RIGHTS {
		return 0, fmt.Errorf("getConnection: recvmsg returned wrong control type: %d", message.Type)
	}
	if oobn <= syscall.SizeofCmsghdr {
		return 0, fmt.Errorf("getConnection: too short control message. Length: %d", oobn)
	}
	if fd < 0 {
		return 0, fmt.Errorf("getConnection: fd < 0: %d", fd)
	}
	return int(fd), nil
}