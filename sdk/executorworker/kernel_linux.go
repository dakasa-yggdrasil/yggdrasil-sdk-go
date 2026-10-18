//go:build linux

package executorworker

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// LinuxKernel requires a dedicated delegated cgroup v2 root. The worker itself
// must live outside it. No Docker socket or privileged command may be exposed.
type LinuxKernel struct {
	Root   string
	rootID string
}

func NewLinuxKernel(root string) (*LinuxKernel, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == "/sys/fs/cgroup" {
		return nil, ErrHold
	}
	b, e := os.ReadFile("/proc/self/mountinfo")
	if e != nil {
		return nil, e
	}
	valid := false
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) > 6 && strings.Contains(line, " - cgroup2 ") && (root == f[4] || strings.HasPrefix(root, f[4]+"/")) {
			valid = true
		}
	}
	if !valid {
		return nil, ErrHold
	}
	for p := root; ; p = filepath.Dir(p) {
		var st syscall.Stat_t
		if e := syscall.Lstat(p, &st); e != nil || st.Uid != 0 || st.Mode&0022 != 0 || st.Mode&syscall.S_IFMT == syscall.S_IFLNK {
			return nil, ErrHold
		}
		if _, e := os.Stat(filepath.Join(p, "cgroup.procs")); e == nil {
			for _, ctl := range []string{"cgroup.procs", "cgroup.threads"} {
				var st syscall.Stat_t
				if e := syscall.Stat(filepath.Join(p, ctl), &st); e != nil || st.Uid != 0 || st.Mode&0022 != 0 {
					return nil, ErrHold
				}
			}
		}
		if p == "/" {
			break
		}
	}
	f, e := os.Open(root)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	var st syscall.Stat_t
	if e = syscall.Fstat(int(f.Fd()), &st); e != nil {
		return nil, e
	}
	k := &LinuxKernel{root, fmt.Sprintf("%d:%d", st.Dev, st.Ino)}
	_, _, e = k.Identity()
	return k, e
}
func (k *LinuxKernel) Identity() (string, string, error) {
	b, e := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if e != nil {
		return "", "", e
	}
	var st syscall.Stat_t
	if e = syscall.Stat(k.Root, &st); e != nil {
		return "", "", e
	}
	id := fmt.Sprintf("%d:%d", st.Dev, st.Ino)
	if id != k.rootID {
		return "", "", ErrHold
	}
	return strings.TrimSpace(string(b)), id, nil
}
func (k *LinuxKernel) Census() (map[string]int, error) {
	out := map[string]int{}
	visited, processes := 0, 0
	e := filepath.WalkDir(k.Root, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.Type()&os.ModeSymlink != 0 {
			return ErrHold
		}
		visited++
		if visited > 4096 {
			return ErrHold
		}
		if !d.IsDir() {
			return nil
		}
		b, e := os.ReadFile(filepath.Join(p, "cgroup.procs"))
		if e != nil {
			return e
		}
		rel, _ := filepath.Rel(k.Root, p)
		parts := strings.Split(rel, string(os.PathSeparator))
		id := parts[0]
		if rel == "." {
			if len(strings.Fields(string(b))) > 0 {
				return ErrHold
			}
			return nil
		}
		if !name.MatchString(id) {
			return ErrHold
		}
		for _, pid := range strings.Fields(string(b)) {
			if n, e := strconv.Atoi(pid); e != nil || n < 1 {
				return ErrHold
			}
			processes++
			if processes > 65536 {
				return ErrHold
			}
			out[id]++
		}
		if _, ok := out[id]; !ok {
			out[id] = 0
		}
		return nil
	})
	return out, e
}
func (k *LinuxKernel) Spawn(id string, c Command) (<-chan error, error) {
	if !name.MatchString(id) || c.UID == 0 || c.GID == 0 {
		return nil, ErrHold
	}
	p := filepath.Join(k.Root, id)
	if e := os.Mkdir(p, 0755); e != nil {
		return nil, e
	}
	// Root-owned directories prevent the unprivileged child moving itself or peers.
	var st syscall.Stat_t
	if e := syscall.Stat(p, &st); e != nil || st.Uid != 0 || st.Mode&0022 != 0 {
		return nil, ErrHold
	}
	f, e := os.Open(p)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	cmd := exec.Command("/usr/bin/setpriv", append([]string{"--no-new-privs", "--", c.Path}, c.Args...)...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=/nonexistent"}
	cmd.Dir = "/"
	cmd.SysProcAttr = &syscall.SysProcAttr{UseCgroupFD: true, CgroupFD: int(f.Fd()), Credential: &syscall.Credential{Uid: c.UID, Gid: c.GID, Groups: []uint32{}, NoSetGroups: false}, Setsid: true}
	// The binary and every parent must be operator-owned and not writable by jobs.
	for _, executable := range []string{"/usr/bin/setpriv", c.Path} {
		for p := executable; ; p = filepath.Dir(p) {
			var x syscall.Stat_t
			if e := syscall.Lstat(p, &x); e != nil || x.Uid != 0 || x.Mode&0022 != 0 || x.Mode&syscall.S_IFMT == syscall.S_IFLNK {
				return nil, ErrHold
			}
			if p == "/" {
				break
			}
		}
	}
	if e = cmd.Start(); e != nil {
		return nil, e
	}
	done := make(chan error, 1)
	go func() {
		e := cmd.Wait()
		counts, ce := k.Census()
		if ce != nil || counts[id] != 0 {
			e = fmt.Errorf("%w: descendants remain or census lost", ErrHold)
		}
		done <- e
		close(done)
	}()
	return done, nil
}
func lockRoot(root string) (*os.File, error) {
	f, e := os.OpenFile(filepath.Join(root, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return nil, e
	}
	return f, nil
}

func validateStateRoot(root string) error {
	for p := root; ; p = filepath.Dir(p) {
		var st syscall.Stat_t
		if e := syscall.Lstat(p, &st); e != nil || st.Uid != 0 || st.Mode&0022 != 0 || st.Mode&syscall.S_IFMT != syscall.S_IFDIR {
			return ErrHold
		}
		if p == "/" {
			break
		}
	}
	for _, base := range []string{"journal.json", "lock"} {
		var st syscall.Stat_t
		e := syscall.Lstat(filepath.Join(root, base), &st)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil || st.Uid != 0 || st.Mode&0077 != 0 || st.Mode&syscall.S_IFMT != syscall.S_IFREG || st.Nlink != 1 {
			return ErrHold
		}
	}
	return nil
}
