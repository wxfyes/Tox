package v2raygrpclite

import (
	std_bufio "bufio"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/sagernet/sing/common"
	"github.com/sagernet/sing/common/baderror"
	"github.com/sagernet/sing/common/buf"
	M "github.com/sagernet/sing/common/metadata"
	"github.com/sagernet/sing/common/varbin"
)

// kanged from: https://github.com/Qv2ray/gun-lite

var grpcObfsKey = []byte("MOMclashGRPCObfuscationKey")

func xorInPlace(data []byte, key []byte) {
	for i := 0; i < len(data); i++ {
		data[i] ^= key[i%len(key)]
	}
}

var _ net.Conn = (*GunConn)(nil)

type GunConn struct {
	rawReader     io.Reader
	reader        *std_bufio.Reader
	writer        io.Writer
	flusher       http.Flusher
	create        chan struct{}
	err           error
	readRemaining int
	obfuscated    bool
}

func newGunConn(reader io.Reader, writer io.Writer, flusher http.Flusher, obfuscated bool) *GunConn {
	return &GunConn{
		rawReader:  reader,
		reader:     std_bufio.NewReader(reader),
		writer:     writer,
		flusher:    flusher,
		obfuscated: obfuscated,
	}
}

func newLateGunConn(writer io.Writer, obfuscated bool) *GunConn {
	return &GunConn{
		create:     make(chan struct{}),
		writer:     writer,
		obfuscated: obfuscated,
	}
}

func (c *GunConn) setup(reader io.Reader, err error) {
	if reader != nil {
		c.rawReader = reader
		c.reader = std_bufio.NewReader(reader)
	}
	c.err = err
	close(c.create)
}

func (c *GunConn) Read(b []byte) (n int, err error) {
	n, err = c.read(b)
	return n, baderror.WrapH2(err)
}

func (c *GunConn) read(b []byte) (n int, err error) {
	if c.reader == nil {
		<-c.create
		if c.err != nil {
			return 0, c.err
		}
	}

	if c.readRemaining > 0 {
		if len(b) > c.readRemaining {
			b = b[:c.readRemaining]
		}
		n, err = c.reader.Read(b)
		c.readRemaining -= n
		if c.obfuscated && n > 0 {
			xorInPlace(b[:n], grpcObfsKey)
		}
		return
	}

	_, err = c.reader.Discard(6)
	if err != nil {
		return
	}

	dataLen, err := binary.ReadUvarint(c.reader)
	if err != nil {
		return
	}

	readLen := int(dataLen)
	c.readRemaining = readLen
	if len(b) > readLen {
		b = b[:readLen]
	}

	n, err = c.reader.Read(b)
	c.readRemaining -= n
	if c.obfuscated && n > 0 {
		xorInPlace(b[:n], grpcObfsKey)
	}
	return
}

func (c *GunConn) Write(b []byte) (n int, err error) {
	varLen := varbin.UvarintLen(uint64(len(b)))
	buffer := buf.NewSize(6 + varLen + len(b))
	header := buffer.Extend(6 + varLen)
	header[0] = 0x00
	binary.BigEndian.PutUint32(header[1:5], uint32(1+varLen+len(b)))
	header[5] = 0x0A
	binary.PutUvarint(header[6:], uint64(len(b)))
	payload := make([]byte, len(b))
	copy(payload, b)
	if c.obfuscated {
		xorInPlace(payload, grpcObfsKey)
	}
	common.Must1(buffer.Write(payload))
	_, err = c.writer.Write(buffer.Bytes())
	if err != nil {
		return 0, baderror.WrapH2(err)
	}
	if c.flusher != nil {
		c.flusher.Flush()
	}
	return len(b), nil
}

func (c *GunConn) WriteBuffer(buffer *buf.Buffer) error {
	defer buffer.Release()
	dataLen := buffer.Len()
	varLen := varbin.UvarintLen(uint64(dataLen))
	if c.obfuscated && dataLen > 0 {
		xorInPlace(buffer.Bytes(), grpcObfsKey)
	}
	header := buffer.ExtendHeader(6 + varLen)
	header[0] = 0x00
	binary.BigEndian.PutUint32(header[1:5], uint32(1+varLen+dataLen))
	header[5] = 0x0A
	binary.PutUvarint(header[6:], uint64(dataLen))
	err := common.Error(c.writer.Write(buffer.Bytes()))
	if err != nil {
		return baderror.WrapH2(err)
	}
	if c.flusher != nil {
		c.flusher.Flush()
	}
	return nil
}

func (c *GunConn) FrontHeadroom() int {
	return 6 + binary.MaxVarintLen64
}

func (c *GunConn) Close() error {
	return common.Close(c.rawReader, c.writer)
}

func (c *GunConn) LocalAddr() net.Addr {
	return M.Socksaddr{}
}

func (c *GunConn) RemoteAddr() net.Addr {
	return M.Socksaddr{}
}

func (c *GunConn) SetDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *GunConn) SetReadDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *GunConn) SetWriteDeadline(t time.Time) error {
	return os.ErrInvalid
}

func (c *GunConn) NeedAdditionalReadDeadline() bool {
	return true
}
