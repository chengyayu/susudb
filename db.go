package susudb

import (
	"io"
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
