package btree

import (
	"bytes"
	"encoding/binary"
)

func nodeLookupLE(node BNode, key []byte) uint16 {
	keys := node.nkeys()
	var low, high uint16 = 0, keys

	for low < high {
		mid := (low + high) / 2
		cmp := bytes.Compare(node.getKey(mid), key)
		if cmp == 0 {
			return mid
		} else if cmp < 0 {
			low = mid + 1
		} else {
			high = mid
		}
	}

	return low
}

func leafNodeInsert(node BNode, old BNode, index uint16, key, value []byte) {
	node.setHeader(NodeLeaf, old.nkeys()+1)
	nodeAppendRange(node, 0, old, 0, index)
	nodeAppendKV(node, index, 0, key, value)
	nodeAppendRange(node, index+1, old, index, old.nkeys()-index)
}

func nodeAppendRange(dst BNode, dstIndex uint16, src BNode, srcIndex, length uint16) {
	if length == 0 {
		return
	}

	for i := uint16(0); i < length; i++ {
		dst.setPtr(dstIndex+i, src.getPtr(srcIndex+i))
	}

	dstKeyOffsetStart := dst.getKeyOffset(dstIndex)
	srcKeyOffsetStart := src.getKeyOffset(srcIndex)
	for i := uint16(1); i <= length; i++ {
		offset := dstKeyOffsetStart + (src.getKeyOffset(srcIndex+i) - srcKeyOffsetStart)
		dst.setKeyOffset(dstIndex+i, offset)
	}

	kvStart := src.kvPosition(srcIndex)
	kvEnd := src.kvPosition(srcIndex + length)
	copy(dst.data[dst.kvPosition(dstIndex):], src.data[kvStart:kvEnd])
}

func nodeAppendKV(node BNode, index uint16, ptr uint64, key, value []byte) {
	node.setPtr(index, ptr)

	pos := node.kvPosition(index)
	binary.LittleEndian.PutUint16(node.data[pos:pos+2], uint16(len(key)))
	binary.LittleEndian.PutUint16(node.data[pos+2:pos+4], uint16(len(value)))
	copy(node.data[pos+4:], key)
	copy(node.data[pos+4+uint16(len(key)):], value)

	node.setKeyOffset(index+1, node.getKeyOffset(index)+uint16(4+len(key)+len(value)))
}
