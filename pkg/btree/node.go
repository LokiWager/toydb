package btree

import "encoding/binary"

type (
	BNode struct {
		data []byte
	}
)

const (
	NodeLeaf     = 0x01
	NodeInternal = 0x02

	HEADER = 4

	PageSize   = 4096
	MaxKeySize = 1000
	MaxValSize = 3000
)

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

func (node BNode) getPtr(index uint16) uint64 {
	offset := HEADER + int(index)*8
	return binary.LittleEndian.Uint64(node.data[offset : offset+8])
}

func (node BNode) setPtr(index uint16, ptr uint64) {
	offset := HEADER + int(index)*8
	binary.LittleEndian.PutUint64(node.data[offset:offset+8], ptr)
}

func (node BNode) getKeyOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	offset := HEADER + int(node.nkeys())*8 + int(index-1)*2
	return binary.LittleEndian.Uint16(node.data[offset : offset+2])
}

func (node BNode) setKeyOffset(index uint16, keyOffset uint16) {
	offset := HEADER + int(node.nkeys())*8 + int(index-1)*2
	binary.LittleEndian.PutUint16(node.data[offset:offset+2], keyOffset)
}

func (node BNode) kvPosition(index uint16) uint16 {
	return HEADER + node.nkeys()*8 + node.nkeys()*2 + node.getKeyOffset(index)
}

func (node BNode) getKey(index uint16) []byte {
	start := node.kvPosition(index)
	keyLen := binary.LittleEndian.Uint16(node.data[start : start+2])
	return node.data[start+4:][:keyLen]
}

func (node BNode) getValue(index uint16) []byte {
	start := node.kvPosition(index)
	keyLen := binary.LittleEndian.Uint16(node.data[start : start+2])
	valLen := binary.LittleEndian.Uint16(node.data[start+2 : start+4])
	return node.data[start+4+keyLen : start+4+keyLen+valLen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPosition(node.nkeys())
}
