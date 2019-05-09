/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fsblkstorage

import (
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
)

var lock = sync.RWMutex{}

type batchedBlockfileWriter struct {
	batch         int
	bfw           *blockfileWriter
	buffer        []writeInfo
	currentLen    int
	currentBuffer []byte
	updated       chan struct{}
}

type writeInfo struct {
	file *os.File
	data []byte
}

func newBatchedBlockFileWriter(bfw *blockfileWriter, batch int) *batchedBlockfileWriter {
	b := &batchedBlockfileWriter{bfw: bfw, batch: batch, buffer: make([]writeInfo, 0, batch), updated: make(chan struct{})}

	go b.finalWrite()

	return b
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

	if w.currentBuffer == nil {
		w.currentBuffer = make([]byte, 0, len(b))
	}

	w.currentBuffer = append(w.currentBuffer, b...)

	if sync {
		w.buffer = append(w.buffer, writeInfo{file: w.bfw.file, data: append([]byte(nil), w.currentBuffer...)})
		w.currentBuffer = w.currentBuffer[:0]
	}

	if len(w.buffer) == w.batch {
		if err := w.writeOut(true); err != nil {
			return err
		}
	}

	w.currentLen += len(b)

	return nil
}

func (w *batchedBlockfileWriter) finalWrite() {

	for {
		select {
		case <-time.After(time.Second * 10):
			if err := w.writeOut(false); err != nil {
				logger.Errorf("Error in batched write: %v", err)
			}
		case <-w.updated:
			return
		}
	}
}

func (w *batchedBlockfileWriter) close() {
	w.bfw.close()
}

func (w *batchedBlockfileWriter) writeOut(wait bool) error {

	//lock.Lock()

	//start := time.Now()

	if wait {
		go w.finalWrite()
	}

	w.updated <- struct{}{}

	var err error

	var lastFile *os.File

	for _, v := range w.buffer {

		if lastFile != nil && lastFile.Name() != v.file.Name() {
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

	if lastFile != nil {
		if err = lastFile.Sync(); err != nil {
			return err
		}
	}

	//logger.Errorf("wr,%d,%d,%.2f\n", time.Now().UnixNano(), len(w.buffer), time.Since(start).Seconds()*1000)

	w.buffer = w.buffer[:0]

	//lock.Unlock()

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
