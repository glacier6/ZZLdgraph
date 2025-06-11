/*
 * Copyright 2025 Hypermode Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codec

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
	"unsafe"

	"github.com/dgryski/go-groupvarint"

	"github.com/dgraph-io/ristretto/v2/z"
	"github.com/hypermodeinc/dgraph/v24/protos/pb"
	"github.com/hypermodeinc/dgraph/v24/x"
)

type seekPos int

const (
	// SeekStart is used with Seek() to search relative to the Uid, returning it in the results.
	// SeekStart与Seek（）一起使用，以相对于Uid进行搜索，并在结果中返回它。
	SeekStart seekPos = iota
	// SeekCurrent to Seek() a Uid using it as offset, not as part of the results.
	// SeekCurrent to Seek（）使用Uid作为偏移量，而不是作为结果的一部分。
	SeekCurrent
)

var (
	bitMask uint64 = 0xffffffff00000000
)

// Encoder is used to convert a list of UIDs into a pb.UidPack object.
type Encoder struct {
	BlockSize int
	pack      *pb.UidPack
	uids      []uint64
	Alloc     *z.Allocator
	buf       *bytes.Buffer
}

var blockSize = int(unsafe.Sizeof(pb.UidBlock{}))

func FreePack(pack *pb.UidPack) {
	if pack == nil {
		return
	}
	if pack.AllocRef == 0 {
		return
	}
	alloc := z.AllocatorFrom(pack.AllocRef)
	alloc.Release()
}

func (e *Encoder) packBlock() {
	if len(e.uids) == 0 {
		return
	}

	// Allocate blocks manually.
	b := e.Alloc.AllocateAligned(blockSize)
	block := (*pb.UidBlock)(unsafe.Pointer(&b[0]))

	block.Base = e.uids[0]
	block.NumUids = uint32(len(e.uids))

	// block := &pb.UidBlock{Base: e.uids[0], NumUids: uint32(len(e.uids))}
	last := e.uids[0]
	e.uids = e.uids[1:]

	e.buf.Reset()
	buf := make([]byte, 17)
	tmpUids := make([]uint32, 4)
	for {
		for i := range 4 {
			if i >= len(e.uids) {
				// Padding with '0' because Encode4 encodes only in batch of 4.
				tmpUids[i] = 0
			} else {
				tmpUids[i] = uint32(e.uids[i] - last)
				last = e.uids[i]
			}
		}

		data := groupvarint.Encode4(buf, tmpUids)
		x.Check2(e.buf.Write(data))

		// e.uids has ended and we have padded tmpUids with 0s
		if len(e.uids) <= 4 {
			e.uids = e.uids[:0]
			break
		}
		e.uids = e.uids[4:]
	}

	sz := len(e.buf.Bytes())
	block.Deltas = e.Alloc.Allocate(sz)
	x.AssertTrue(sz == copy(block.Deltas, e.buf.Bytes()))
	e.pack.Blocks = append(e.pack.Blocks, block)
}

var tagEncoder = "enc"

// Add takes an uid and adds it to the list of UIDs to be encoded.
func (e *Encoder) Add(uid uint64) {
	if e.pack == nil {
		e.pack = &pb.UidPack{BlockSize: uint32(e.BlockSize)}
		e.buf = new(bytes.Buffer)
	}
	if e.Alloc == nil {
		e.Alloc = z.NewAllocator(1024, tagEncoder)
	}

	size := len(e.uids)
	if size > 0 && !match32MSB(e.uids[size-1], uid) {
		e.packBlock()
		e.uids = e.uids[:0]
	}

	e.uids = append(e.uids, uid)
	if len(e.uids) >= e.BlockSize {
		e.packBlock()
		e.uids = e.uids[:0]
	}
}

// Done returns the final output of the encoder. This UidPack MUST BE FREED via a call to FreePack.
func (e *Encoder) Done() *pb.UidPack {
	e.packBlock()
	if e.pack != nil && e.Alloc != nil {
		e.pack.AllocRef = e.Alloc.Ref
	}
	return e.pack
}

// Decoder is used to read a pb.UidPack object back into a list of UIDs.
type Decoder struct {
	Pack     *pb.UidPack
	blockIdx int
	uids     []uint64
}

// NewDecoder returns a decoder for the given UidPack and properly initializes it.
func NewDecoder(pack *pb.UidPack) *Decoder {
	decoder := &Decoder{
		Pack: pack,
	}
	decoder.Seek(0, SeekStart)
	return decoder
}

// 每次解析的是一个block（不是一个pack）
func (d *Decoder) UnpackBlock() []uint64 {
	if len(d.uids) > 0 { // 清空
		// We were previously preallocating the d.uids slice to block size. This caused slowdown
		// because many blocks are small and only contain a few ints, causing wastage while still
		// paying cost of allocation.
		// 我们之前将d.uid切片预分配到块大小。这导致了速度减慢，因为许多块很小，只包含几个整数，造成了浪费，同时仍要支付分配成本。
		d.uids = d.uids[:0]  
	}

	if d.blockIdx >= len(d.Pack.Blocks) { // 当块下标大于总长度时，直接退出
		return d.uids
	}
	block := d.Pack.Blocks[d.blockIdx] // 得到下标块

	last := block.Base
	d.uids = append(d.uids, last)

	tmpUids := make([]uint32, 4)
	var sum uint64
	encData := block.Deltas // 得到待解析数据

	for uint32(len(d.uids)) < block.NumUids {
		if len(encData) < 17 { // 不到17的话，就扩充到17字节
			// Decode4 decodes 4 uids from encData. It moves slice(encData) forward while
			// decoding and expects it to be of length >= 4 at all the stages.
			// The SSE code tries to read 16 bytes past the header(1 byte).
			// So we are padding encData to increase its length to 17 bytes.
			// This is a workaround for https://github.com/dgryski/go-groupvarint/issues/1
			//
			// We should NEVER write to encData, because it references block.Deltas, which is laid
			// out on an allocator.
			// Decode4从encData中解码4个uid。它在解码时向前移动切片（encData），并期望它在所有阶段的长度>=4。
			// SSE代码试图读取超过标头（1字节）的16个字节。
			// 因此，我们正在填充encData，将其长度增加到17个字节。
			// 这是一种解决方法https://github.com/dgryski/go-groupvarint/issues/1
			tmp := make([]byte, 17)
			copy(tmp, encData)
			encData = tmp
		}

		groupvarint.Decode4(tmpUids, encData) // NOTE:核心操作，每次解码出来四个偏移量（groupvarint是高效批量解析连续 Varint 编码整数的工具）
		encData = encData[groupvarint.BytesUsed[encData[0]]:] // 待解码块后移下标
		for i := range 4 {
			sum = last + uint64(tmpUids[i]) // 最终的目标值由base+解码到的偏移量得到，且每一个当前的偏移量的值都是相对于紧挨着上一个确切值的偏移量
			d.uids = append(d.uids, sum)
			last = sum
		}
	}

	d.uids = d.uids[:block.NumUids]
	return d.uids
}

// ApproxLen returns the approximate number of UIDs in the pb.UidPack object.
func (d *Decoder) ApproxLen() int {
	if d == nil {
		return 0
	}
	if d.Pack == nil {
		return 0
	}
	return int(d.Pack.BlockSize) * (len(d.Pack.Blocks) - d.blockIdx)
}

type searchFunc func(int) bool

// SeekToBlock will find the block containing the uid, and unpack it. When we are going to
// intersect the list later, this function is useful. As this function skips the search function
// and returns the entire block, it is faster than Seek. Unlike seek, we don't truncate the uids
// returned, which would be done by the intersect function anyways.
func (d *Decoder) SeekToBlock(uid uint64, whence seekPos) []uint64 {
	if d.Pack == nil {
		return []uint64{}
	}
	prevBlockIdx := d.blockIdx
	d.blockIdx = 0
	if uid == 0 {
		return d.UnpackBlock()
	}

	// If for some reason we are searching an older uid, we need to search the entire pack
	if prevBlockIdx > 0 && uid < d.Pack.Blocks[prevBlockIdx].Base {
		prevBlockIdx = 0
	}

	blocksFunc := func() searchFunc {
		var f searchFunc
		switch whence {
		case SeekStart:
			f = func(i int) bool { return d.Pack.Blocks[i+prevBlockIdx].Base >= uid }
		case SeekCurrent:
			f = func(i int) bool { return d.Pack.Blocks[i+prevBlockIdx].Base > uid }
		}
		return f
	}

	idx := sort.Search(len(d.Pack.Blocks[prevBlockIdx:]), blocksFunc()) + prevBlockIdx
	// The first block.Base >= uid.
	if idx == 0 {
		return d.UnpackBlock()
	}
	// The uid is the first entry in the block.
	if idx < len(d.Pack.Blocks) && d.Pack.Blocks[idx].Base == uid {
		d.blockIdx = idx
		return d.UnpackBlock()
	}

	// Either the idx = len(pack.Blocks) that means it wasn't found in any of the block's base. Or,
	// we found the first block index whose base is greater than uid. In these cases, go to the
	// previous block and search there.
	d.blockIdx = idx - 1 // Move to the previous block. If blockIdx<0, unpack will deal with it.
	if d.blockIdx != prevBlockIdx {
		d.UnpackBlock() // And get all their uids.
	}

	if uid <= d.uids[len(d.uids)-1] {
		return d.uids
	}

	// Could not find any uid in the block, which is >= uid. The next block might still have valid
	// entries > uid.
	return d.Next()
}

// Seek will search for uid in a packed block using the specified whence position.
// The value of whence must be one of the predefined values SeekStart or SeekCurrent.
// SeekStart searches uid and includes it as part of the results.
// SeekCurrent searches uid but only as offset, it won't be included with results.
//
// Returns a slice of all uids whence the position, or an empty slice if none found.

// Seek将使用指定的起始位置在打包块中搜索uid。
// whence的值必须是预定义值SeekStart或SeekCurrent之一。
// SeekStart搜索uid并将其作为结果的一部分。 （本行以及下一行的uid均指的是参数里面的那个uid）
// SeekCurrent搜索uid，但仅作为偏移量，它不会包含在结果中。
// 返回位置所在的所有uid的切片，如果没有找到，则返回空切片。
func (d *Decoder) Seek(uid uint64, whence seekPos) []uint64 {
	if d.Pack == nil {
		return []uint64{}
	}
	d.blockIdx = 0
	if uid == 0 {
		return d.UnpackBlock()
	}

	pack := d.Pack // 得到打包块（密文）
	blocksFunc := func() searchFunc {
		var f searchFunc
		switch whence {
		case SeekStart:
			f = func(i int) bool { return pack.Blocks[i].Base >= uid }
		case SeekCurrent:
			f = func(i int) bool { return pack.Blocks[i].Base > uid }
		}
		return f
	}

	idx := sort.Search(len(pack.Blocks), blocksFunc()) // 找到目标块下标（通过判断uid的偏移量）
	// The first block.Base >= uid.
	if idx == 0 { // 如果第一个block都满足，那么直接解码即可
		return d.UnpackBlock() // NOTE:核心操作，解析为明文uid列表 zzlTODO:看这里面怎么解析的
	}
	// The uid is the first entry in the block.
	// 如果uid的值刚好等于当前下标块的起始UID的值
	if idx < len(pack.Blocks) && pack.Blocks[idx].Base == uid {
		d.blockIdx = idx
		return d.UnpackBlock()
	}

	// Either the idx = len(pack.Blocks) that means it wasn't found in any of the block's base. Or,
	// we found the first block index whose base is greater than uid. In these cases, go to the
	// previous block and search there.
	// 要么是idx=len（pack.Blocks），这意味着在任何块的base中都找不到它。或者，我们找到了第一个基数大于uid的块索引。在这些情况下，转到上一个块并在那里搜索。
	d.blockIdx = idx - 1 // Move to the previous block. If blockIdx<0, unpack will deal with it. 移动到上一个块。如果blockIdx<0，unpack将处理它。
	d.UnpackBlock()      // And get all their uids. 并获取他们所有的UID。

	uidsFunc := func() searchFunc { // 为了找上一个块中第一个满足条件的uid
		var f searchFunc
		switch whence {
		case SeekStart:
			f = func(i int) bool { return d.uids[i] >= uid }
		case SeekCurrent:
			f = func(i int) bool { return d.uids[i] > uid }
		}
		return f
	}

	// uidx points to the first uid in the uid list, which is >= uid.
	// uidx指向uid列表中的第一个>=参数uid的uid。
	uidx := sort.Search(len(d.uids), uidsFunc())
	if uidx < len(d.uids) { // Found an entry in uids, which >= uid.
		d.uids = d.uids[uidx:]
		return d.uids
	}
	// Could not find any uid in the block, which is >= uid. The next block might still have valid
	// entries > uid.
	// 在块中找不到任何uid，即>=uid。下一个块可能仍有有效条目>uid。
	return d.Next()
}

// Uids returns all the uids in the pb.UidPack object as an array of integers.
// uids are owned by the Decoder, and the slice contents would be changed on the next call. They
// should be copied if passed around.
func (d *Decoder) Uids() []uint64 {
	return d.uids
}

// LinearSeek returns uids of the last block whose base is less than seek.
// If there are no such blocks i.e. seek < base of first block, it returns uids of first
// block. LinearSeek is used to get closest uids which are >= seek.
func (d *Decoder) LinearSeek(seek uint64) []uint64 {
	for {
		v := d.PeekNextBase()
		if seek < v {
			break
		}
		d.blockIdx++
	}

	return d.UnpackBlock()
}

// PeekNextBase returns the base of the next block without advancing the decoder.
func (d *Decoder) PeekNextBase() uint64 {
	bidx := d.blockIdx + 1
	if bidx < len(d.Pack.Blocks) {
		return d.Pack.Blocks[bidx].Base
	}
	return math.MaxUint64
}

// Valid returns true if the decoder has not reached the end of the packed data.
func (d *Decoder) Valid() bool {
	return d.blockIdx < len(d.Pack.Blocks)
}

// Next moves the decoder on to the next block.
func (d *Decoder) Next() []uint64 {
	d.blockIdx++
	return d.UnpackBlock()
}

// BlockIdx returns the index of the block that is currently being decoded.
func (d *Decoder) BlockIdx() int {
	return d.blockIdx
}

// Encode takes in a list of uids and a block size. It would pack these uids into blocks of the
// given size, with the last block having fewer uids. Within each block, it stores the first uid as
// base. For each next uid, a delta = uids[i] - uids[i-1] is stored. Protobuf uses Varint encoding,
// as mentioned here: https://developers.google.com/protocol-buffers/docs/encoding . This ensures
// that the deltas being considerably smaller than the original uids are nicely packed in fewer
// bytes. Our benchmarks on artificial data show compressed size to be 13% of the original. This
// mechanism is a LOT simpler to understand and if needed, debug.
func Encode(uids []uint64, blockSize int) *pb.UidPack {
	enc := Encoder{BlockSize: blockSize}
	for _, uid := range uids {
		enc.Add(uid)
	}
	return enc.Done()
}

// EncodeFromBuffer is the same as Encode but it accepts a byte slice instead of a uint64 slice.
func EncodeFromBuffer(buf []byte, blockSize int) *pb.UidPack {
	enc := Encoder{BlockSize: blockSize}
	var prev uint64
	for len(buf) > 0 {
		uid, n := binary.Uvarint(buf)
		buf = buf[n:]

		next := prev + uid
		enc.Add(next)
		prev = next
	}
	return enc.Done()
}

// ApproxLen would indicate the total number of UIDs in the pack. Can be used for int slice
// allocations.
// ApproxLen将指示包中UID的总数。可用于int切片分配。
func ApproxLen(pack *pb.UidPack) int {
	if pack == nil {
		return 0
	}
	return len(pack.Blocks) * int(pack.BlockSize)
}

// ExactLen would calculate the total number of UIDs. Instead of using a UidPack, it accepts blocks,
// so we can calculate the number of uids after a seek.
func ExactLen(pack *pb.UidPack) int {
	if pack == nil {
		return 0
	}
	sz := len(pack.Blocks)
	if sz == 0 {
		return 0
	}
	num := 0
	for _, b := range pack.Blocks {
		num += int(b.NumUids) // NumUids includes the base UID.
	}
	return num
}

// Decode decodes the UidPack back into the list of uids. This is a stop-gap function, Decode would
// need to do more specific things than just return the list back.
func Decode(pack *pb.UidPack, seek uint64) []uint64 {
	out := make([]uint64, 0, ApproxLen(pack))
	dec := Decoder{Pack: pack}

	for uids := dec.Seek(seek, SeekStart); len(uids) > 0; uids = dec.Next() {
		out = append(out, uids...)
	}
	return out
}

// DecodeToBuffer is the same as Decode but it returns a z.Buffer which is
// calloc'ed and can be SHOULD be freed up by calling buffer.Release().
func DecodeToBuffer(buf *z.Buffer, pack *pb.UidPack) {
	var last uint64
	tmp := make([]byte, 16)
	dec := Decoder{Pack: pack}
	for uids := dec.Seek(0, SeekStart); len(uids) > 0; uids = dec.Next() {
		for _, u := range uids {
			n := binary.PutUvarint(tmp, u-last)
			x.Check2(buf.Write(tmp[:n]))
			last = u
		}
	}
}

func match32MSB(num1, num2 uint64) bool {
	return (num1 & bitMask) == (num2 & bitMask)
}

// CopyUidPack creates a copy of the given UidPack.
func CopyUidPack(pack *pb.UidPack) *pb.UidPack {
	if pack == nil {
		return nil
	}

	packCopy := new(pb.UidPack)
	packCopy.BlockSize = pack.BlockSize
	packCopy.Blocks = make([]*pb.UidBlock, len(pack.Blocks))

	for i, block := range pack.Blocks {
		packCopy.Blocks[i] = new(pb.UidBlock)
		packCopy.Blocks[i].Base = block.Base
		packCopy.Blocks[i].NumUids = block.NumUids
		packCopy.Blocks[i].Deltas = make([]byte, len(block.Deltas))
		copy(packCopy.Blocks[i].Deltas, block.Deltas)
	}

	return packCopy
}
