/*
@Time : 2021/12/8 11:44
@Author : Dao
@File : record
@Software: GoLand
*/

package model

import (
	"dududb/common"
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	// KeySize is uint32 type, with 4 bytes.
	keySizeLen = 4
	// ValueSize is uint32 type, with 4 bytes.
	valueSizeLen = 4
	// crc32 is uint32 type, with 4 bytes.
	crc32Len = 4
	// TimeStamp is uint32 type, with 4 bytes.
	timeStampLen = 8

	// recordBaseSize is the fundamental size of per record
	// include:
	// 		keySizeLen, valueSizeLen, crc32Len, timeStampLen
	recordBaseSize = keySizeLen + valueSizeLen + crc32Len + timeStampLen
)

// Meta meta data
type Meta struct {
	Key       []byte
	Value     []byte
	KeySize   uint32
	ValueSize uint32
}

// Record a data record be append to db file
type Record struct {
	Meta      *Meta
	crc32     uint32
	TimeStamp uint64
}

// NewRecord returns a new record created
func NewRecord(k, v []byte) (*Record, error) {
	if len(k) == 0 || len(v) == 0 {
		return nil, common.ErrInvalidRecord
	}

	var e = &Record{
		Meta: &Meta{
			Key:       k,
			Value:     v,
			KeySize:   uint32(len(k)),
			ValueSize: uint32(len(v)),
		},
		TimeStamp: uint64(time.Now().UnixNano()),
	}
	e.crc32 = e.checkSumCrc32()
	return e, nil
}

// checkSumCrc32 returns the CRC-32 checksum of record
func (r *Record) checkSumCrc32() uint32 {
	return crc32.ChecksumIEEE(r.Meta.Value)
}

// Size return bytes of record when be packed into byte array.
func (r *Record) Size() uint32 {
	return recordBaseSize + r.Meta.KeySize + r.Meta.ValueSize
}

// Pack pack the record and returns byte array.
func (r *Record) Pack() ([]byte, error) {
	buf := make([]byte, r.Size())
	binary.LittleEndian.PutUint32(buf[0:crc32Len], r.crc32)
	binary.LittleEndian.PutUint64(buf[crc32Len:crc32Len+timeStampLen], r.TimeStamp)
	binary.LittleEndian.PutUint32(buf[crc32Len+timeStampLen:crc32Len+timeStampLen+keySizeLen], r.Meta.KeySize)
	binary.LittleEndian.PutUint32(buf[crc32Len+timeStampLen+keySizeLen:recordBaseSize], r.Meta.ValueSize)
	copy(buf[recordBaseSize:recordBaseSize+r.Meta.KeySize], r.Meta.Key)
	copy(buf[recordBaseSize+r.Meta.KeySize:recordBaseSize+r.Meta.KeySize+r.Meta.ValueSize], r.Meta.Value)

	return buf, nil
}

// Unpack unpack a byte array and returns the record.
func Unpack(buf []byte) (*Record, error) {
	crc := binary.LittleEndian.Uint32(buf[0:crc32Len])
	ts := binary.LittleEndian.Uint64(buf[crc32Len : crc32Len+timeStampLen])
	ks := binary.LittleEndian.Uint32(buf[crc32Len+timeStampLen : crc32Len+timeStampLen+keySizeLen])
	vs := binary.LittleEndian.Uint32(buf[crc32Len+timeStampLen+keySizeLen : recordBaseSize])
	kc := make([]byte, ks)
	vc := make([]byte, vs)
	copy(kc, buf[recordBaseSize:recordBaseSize+ks])
	copy(vc, buf[recordBaseSize+ks:recordBaseSize+ks+vs])

	return &Record{
		Meta: &Meta{
			KeySize:   ks,
			ValueSize: vs,
			Key:       kc,
			Value:     vc,
		},
		crc32:     crc,
		TimeStamp: ts,
	}, nil
}
