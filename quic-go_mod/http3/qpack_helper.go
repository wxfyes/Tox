package http3

import (
	"io"
	"github.com/quic-go/qpack"
)

func decodeFull(decoder *qpack.Decoder, p []byte) ([]qpack.HeaderField, error) {
	decode := decoder.Decode(p)
	var hfs []qpack.HeaderField
	for {
		hf, err := decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		hfs = append(hfs, hf)
	}
	return hfs, nil
}
