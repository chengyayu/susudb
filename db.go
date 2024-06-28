package susudb

import (
	"fmt"
	"io"
	"math/rand"
	"os"
)

// SaveData1 naive approach
// some drawbacks:
// 1. truncates the file before updating it. what if the file needs to be read concurrently?
// 2. writing data to files may not atomic, depending on the size of the write. concurrent readers might get incomplete data.
// 3. the data is probably still in the os's page cache after the write syscall returns. what's the state of the file when the system crashes and reboots?
func SaveData1(path string, data []byte) (err error) {
	closeFn := func(c io.Closer) {
		closeErr := c.Close()
		if err != nil {
			return
		}
		if closeErr != nil {
			err = closeErr
			return
		}
	}

	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer closeFn(fp)

	_, err = fp.Write(data)
	return err
}

// SaveData2 a little better approach
// 改进：
// 1. the rename operation is atomic.
// If the system crashed before renaming, the original file remains intact, and applications have no problem reading the file concurrently.
// 问题：
// 1. 何时持久化到磁盘？metadata 可能早于 data 持久化到磁盘，系统崩溃后可能损坏文件。
func SaveData2(path string, data []byte) (err error) {
	closeFn := func(c io.Closer) {
		closeErr := c.Close()
		if err != nil {
			return
		}
		if closeErr != nil {
			err = closeErr
			return
		}
	}

	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer closeFn(fp)

	_, err = fp.Write(data)
	if err != nil {
		os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, path)
}

func randomInt() int {
	return rand.Intn(100)
}

// SaveData3 a little better approach than SaveData2
// 改进：
// 1. 重命名之前，将数据刷到磁盘上。
// 问题：
// 1. metadata 何时刷盘？需要包含该文件的所有目录也刷盘吗？
// 这似乎是一个兔子洞般深坑，这就是为什么人们在做数据持久化时更青睐于 database 而非 files.
func SaveData3(path string, data []byte) (err error) {
	closeFn := func(c io.Closer) {
		closeErr := c.Close()
		if err != nil {
			return
		}
		if closeErr != nil {
			err = closeErr
			return
		}
	}

	tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
	fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer closeFn(fp)

	_, err = fp.Write(data)
	if err != nil {
		os.Remove(tmp)
		return err
	}

	if err := fp.Sync(); err != nil {
		os.Remove(tmp)
		return err
	}

	return os.Rename(tmp, path)

}
