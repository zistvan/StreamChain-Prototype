/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fsblkstorage

import (
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
)

type batchedBlockfileWriter struct {
	batch      int
	bfw        *blockfileWriter
	buffer     []writeInfo
	currentLen int
	//updated    chan struct{}
}

type writeInfo struct {
	file *os.File
	data []byte
}

func newBatchedBlockFileWriter(bfw *blockfileWriter, batch int) *batchedBlockfileWriter {
	//return &batchedBlockfileWriter{bfw: bfw, batch: batch, buffer: make([]writeInfo, 0, batch), updated: make(chan struct{})}
	return &batchedBlockfileWriter{bfw: bfw, batch: batch, buffer: make([]writeInfo, 0, batch)}
}

func (w *batchedBlockfileWriter) setBlockfileWriter(bfw *blockfileWriter) {
	w.bfw.close()
	w.currentLen = 0
	w.bfw = bfw
}

func (w *batchedBlockfileWriter) append(b []byte, sync bool) error {

	if w.batch == 0 {
		return w.bfw.append(b, sync)
	}

	if sync {
		w.buffer = append(w.buffer, writeInfo{file: w.bfw.file, data: b})
	} else {
		if len(w.buffer) > 0 {
			last := w.buffer[len(w.buffer)-1]
			last.data = append(last.data, b...)
		} else {
			w.buffer = append(w.buffer, writeInfo{file: w.bfw.file, data: b})
		}
	}

	if len(w.buffer) == w.batch {
		if err := w.writeOut(true); err != nil {
			return err
		}

		//go w.writeOut(true)
	}

	w.currentLen += len(b)

	return nil
}

/*
func (w *batchedBlockfileWriter) finalWrite() {

	for {
		select {
		case <-time.After(time.Second * 10):
			if err := w.writeOut(false); err != nil {
				logger.Errorf("Error in batched write")
			}
		case <-w.updated:
			return
		}
	}
}
*/
func (w *batchedBlockfileWriter) close() {
	w.bfw.close()
	//close(w.updated)
}

func (w *batchedBlockfileWriter) writeOut(wait bool) error {

	start := time.Now()

	//if wait {
	//	w.updated <- struct{}{}
	//}

	var err error

	var lastFile *os.File

	for _, v := range w.buffer {

		if lastFile != nil && lastFile != v.file {
			if err = lastFile.Sync(); err != nil {
				return err
			}
		}

		_, err = v.file.Write(v.data)

		if err != nil {
			return err
		}

		lastFile = v.file
	}

	if err = lastFile.Sync(); err != nil {
		return err
	}

	//if wait {
	//	go w.finalWrite()
	//}

	fmt.Printf("wr,%d,%d,%.2f\n", time.Now().UnixNano(), len(w.buffer), time.Since(start).Seconds()*1000)

	w.buffer = w.buffer[:0]

	return nil
}

func (w *batchedBlockfileWriter) truncateFile(targetSize int) error {

	if w.batch == 0 {
		return w.bfw.truncateFile(targetSize)
	}

	if w.currentLen > targetSize {
		lastBuf := w.buffer[len(w.buffer)-1].data
		left := w.currentLen - targetSize
		lastBuf = lastBuf[:(len(lastBuf) - left)]
		w.currentLen = targetSize
	}

	return nil
}

////  WRITER ////
type blockfileWriter struct {
	filePath string
	file     *os.File
}

func newBlockfileWriter(filePath string) (*blockfileWriter, error) {
	writer := &blockfileWriter{filePath: filePath}
	return writer, writer.open()
}

func (w *blockfileWriter) truncateFile(targetSize int) error {
	fileStat, err := w.file.Stat()
	if err != nil {
		return err
	}
	if fileStat.Size() > int64(targetSize) {
		w.file.Truncate(int64(targetSize))
	}
	return nil
}

func (w *blockfileWriter) append(b []byte, sync bool) error {
	_, err := w.file.Write(b)
	if err != nil {
		return err
	}
	if sync {
		return w.file.Sync()
	}
	return nil
}

func (w *blockfileWriter) open() error {
	file, err := os.OpenFile(w.filePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return errors.Wrapf(err, "error opening block file writer for file %s", w.filePath)
	}
	w.file = file
	return nil
}

func (w *blockfileWriter) close() error {
	return errors.WithStack(w.file.Close())
}

////  READER ////
type blockfileReader struct {
	file *os.File
}

func newBlockfileReader(filePath string) (*blockfileReader, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0600)
	if err != nil {
		return nil, errors.Wrapf(err, "error opening block file reader for file %s", filePath)
	}
	reader := &blockfileReader{file}
	return reader, nil
}

func (r *blockfileReader) read(offset int, length int) ([]byte, error) {
	b := make([]byte, length)
	_, err := r.file.ReadAt(b, int64(offset))
	if err != nil {
		return nil, errors.Wrapf(err, "error reading block file for offset %d and length %d", offset, length)
	}
	return b, nil
}

func (r *blockfileReader) close() error {
	return errors.WithStack(r.file.Close())
}
