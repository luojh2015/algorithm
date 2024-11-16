package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	ProtocolVersion byte = 1
	MaxKeySize           = 128
	MaxDataSize          = 1 << 20
	HeaderSize           = 12

	TagIndex      = 0
	VersionIndex  = 3
	KeySizeIndex  = 4
	DataSizeIndex = 8

	EnableReadWriteLog = false
)

var (
	// 自定义协议的标志
	ProtocolTag = [VersionIndex]byte{'P', 'R', 'T'} // "PRT"

	// 用缓存池复用内存，节省内存开销
	pl = &sync.Pool{
		New: func() interface{} {
			return &node{
				next: nil,
			}
		},
	}
)

// 自定义协议包头
// 完整的包头为: ProtocolTag + ProtocolVer + KeySize + DataSize + key + data
// 思路：将协议标志、协议版本、key长度、data长度写入header中，然后将key和data依次写到header后面
// 将大的数据分片发送，多个业务逻辑通过抢占锁资源来发送自己的分片数据，数据包大的业务逻辑就不会导致其他数据包小的业务逻辑一直阻塞
// 因为发送方也可能分多次写入数据，并且发送完成是单独的接口调用，所以通过发送一个空数据分片来标志数据发送完了，而不是在header中设置标志位
// 每个链接启动一个读取协程，将读到的分片数据按照key拼接缓存起来
type header struct {
	//ProtocolTag [3]byte
	//ProtocolVer byte
	//KeySize     [4]byte
	//DataSize    [4]byte
	buf [HeaderSize]byte
}

func newHeader(keySize int) *header {
	h := &header{}

	copy(h.buf[TagIndex:], ProtocolTag[:])
	h.buf[VersionIndex] = ProtocolVersion
	copy(h.buf[KeySizeIndex:], []byte{byte(keySize >> 24), byte(keySize >> 16), byte(keySize >> 8), byte(keySize)})

	return h
}

func (h *header) setDataSize(dataSize int) {
	copy(h.buf[DataSizeIndex:], []byte{byte(dataSize >> 24), byte(dataSize >> 16), byte(dataSize >> 8), byte(dataSize)})
}

type sender struct {
	h    *header
	key  []byte
	conn *Conn
	sync.Mutex
	isClosed bool
	log      io.WriteCloser
}

func (s *sender) insertToConnSendBuf() {
	s.conn.Lock()
	defer s.conn.Unlock()
	s.conn.sendBufs[s] = true
}

// 期望将一段数据完整的写入到发送区
func (s *sender) write(p []byte) (n int, err error) {
	for n < len(p) {
		size, err := s.conn.nc.Write(p[n:])
		if err != nil {
			return n, err
		}
		n += size
	}
	return n, nil
}

// 期望将整个分片完整的写入到发送缓冲区
func (s *sender) writeTrunk(trunk []byte) (int, error) {
	s.conn.Lock()
	defer s.conn.Unlock()

	if s.conn.isClosed {
		return 0, fmt.Errorf("write to closed connection")
	}

	s.h.setDataSize(len(trunk))
	if _, err := s.write(s.h.buf[:]); err != nil {
		return 0, err
	}

	if _, err := s.write(s.key); err != nil {
		return 0, err
	}

	if len(trunk) == 0 {
		return 0, nil
	}

	if s.log != nil {
		s.log.Write(trunk)
	}

	return s.write(trunk)
}

func (s *sender) close() {
	s.h.setDataSize(0)
	if _, err := s.write(s.h.buf[:]); err != nil {
		return
	}

	if _, err := s.write(s.key); err != nil {
		return
	}
}

func (s *sender) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return
	}

	s.Lock()
	defer s.Unlock()
	if s.isClosed {
		return 0, fmt.Errorf("WriteCloser is closed")
	}

	s.insertToConnSendBuf()

	dataSize := len(p)
	trunkSize := MaxDataSize

	for i := 0; i < dataSize; i += MaxDataSize {
		if trunkSize > dataSize-i {
			trunkSize = dataSize - i
		}
		if size, err := s.writeTrunk(p[i : i+trunkSize]); err != nil {
			return n + size, err
		} else {
			n += size
		}
	}

	return n, nil
}

func (s *sender) Close() error {
	s.Lock()
	defer s.Unlock()
	if s.isClosed {
		return nil
	}

	// 空数据包表示该key对应的数据发送完了
	if _, err := s.writeTrunk(nil); err != nil {
		return err
	}

	s.conn.Lock()
	defer s.conn.Unlock()
	delete(s.conn.sendBufs, s)
	s.isClosed = true
	if s.log != nil {
		s.log.Close()
	}

	return nil
}

func newSender(conn *Conn, key string) *sender {
	var fd io.WriteCloser
	if EnableReadWriteLog {
		fd, _ = os.OpenFile(key+".log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	}
	return &sender{
		h:    newHeader(len(key)),
		key:  []byte(key),
		conn: conn,
		log:  fd,
	}
}

type receiver struct {
	key    string
	head   *node
	tail   *node
	notify chan struct{}
	log    io.WriteCloser
	*sync.Mutex
}

// 以单向链表的形式存储数据，节省内存消耗
type node struct {
	offset int
	data   []byte
	next   *node
}

func (n *node) reset() {
	n.offset = 0
	n.next = nil
	n.data = n.data[:0]
}

func (r *receiver) read(p []byte) (n int) {
	for n < len(p) && r.head != nil {
		t := r.head
		size := copy(p[n:], t.data[t.offset:])
		n += size
		t.offset += size
		if t.offset == len(t.data) {
			r.Lock()
			r.head = r.head.next
			r.Unlock()
			t.reset()
			pl.Put(t)
		}
	}

	r.Lock()
	if r.head == nil {
		r.tail = nil
	}
	r.Unlock()

	return
}

func (r *receiver) Read(p []byte) (n int, err error) {
	// 如果有数据等待读取了，就直接读
	if r.head != nil {
		return r.read(p), nil
	}

	// 如果没有数据等待，那就等待被通知
	select {
	case _, ok := <-r.notify:
		n = r.read(p)
		// 如果没有读完，还可以继续读
		// 读完之后还是能够得到ch关闭的信号的
		if r.head != nil {
			return n, nil
		}
		if !ok {
			return 0, io.EOF
		}
		return n, err
	}
}

func (r *receiver) Write(p []byte) {
	v := pl.Get()
	n := v.(*node)
	n.data = append(n.data, p...)

	r.Lock()
	defer r.Unlock()
	if r.head == nil {
		r.head = n
		r.tail = n
		return
	}

	r.tail.next = n
	r.tail = n
}

func (r *receiver) Close() {
	close(r.notify)
}

func newReceiver(key string) *receiver {
	var fd io.WriteCloser
	if EnableReadWriteLog {
		fd, _ = os.OpenFile(key+"_read.log", os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
	}
	return &receiver{
		key: key,
		//Buffer: bytes.NewBuffer(nil),
		notify: make(chan struct{}, 1),
		log:    fd,
		Mutex:  &sync.Mutex{},
	}
}

// Conn 是你需要实现的一种连接类型，它支持下面描述的若干接口；
// 为了实现这些接口，你需要设计一个基于 TCP 的简单协议；
type Conn struct {
	nc       net.Conn
	sendBufs map[*sender]bool
	recvBufs map[string]*receiver // 存储接收数据的缓存句柄
	rs       []*receiver
	rlock    *sync.Mutex
	rcond    *sync.Cond
	isClosed bool
	*sync.Mutex
	*sync.WaitGroup
}

func (conn *Conn) IsClosed() bool {
	conn.Lock()
	defer conn.Unlock()
	return conn.isClosed
}

// Send 传入一个 key 表示发送者将要传输的数据对应的标识；
// 返回 writer 可供发送者分多次写入大量该 key 对应的数据；
// 当发送者已将该 key 对应的所有数据写入后，调用 writer.Close 告知接收者：该 key 的数据已经完全写入；
func (conn *Conn) Send(key string) (writer io.WriteCloser, err error) {
	if len(key) == 0 {
		return nil, errors.New("key is empty")
	} else if len(key) > MaxKeySize {
		return nil, fmt.Errorf("key too long, max %d bytes", MaxKeySize)
	}
	return newSender(conn, key), nil
}

// Receive 返回一个 key 表示接收者将要接收到的数据对应的标识；
// 返回的 reader 可供接收者多次读取该 key 对应的数据；
// 当 reader 返回 io.EOF 错误时，表示接收者已经完整接收该 key 对应的数据；
func (conn *Conn) Receive() (key string, reader io.Reader, err error) {
	for {
		key, r, err := conn.waitReader()
		if r == nil && &err == nil {
			continue
		}

		return key, r, err
	}
}

func (conn *Conn) waitReader() (key string, reader io.Reader, err error) {
	conn.rlock.Lock()
	defer conn.rlock.Unlock()

	key, reader, err = conn.getReader()
	if reader == nil && err == nil {
		conn.rcond.Wait()
	} else {
		return
	}

	key, reader, err = conn.getReader()
	return
}

func (conn *Conn) getReader() (key string, reader io.Reader, err error) {
	if len(conn.rs) > 0 {
		r := conn.rs[0]
		if r == nil {
			return "", nil, io.EOF
		}
		conn.rs = conn.rs[1:]
		return r.key, r, nil
	}

	if conn.IsClosed() {
		return "", nil, io.EOF
	}

	return "", nil, nil
}

// Close 关闭你实现的连接对象及其底层的 TCP 连接
func (conn *Conn) Close() {
	conn.close()
	conn.Wait()
}

func (conn *Conn) close() {
	// 通知所有等待接收的都检查一下看看是否还有未处理的数据
	conn.rlock.Lock()
	conn.rcond.Broadcast()
	conn.rlock.Unlock()

	conn.Lock()
	defer conn.Unlock()
	if conn.isClosed {
		return
	}

	// 所有发送缓冲区的消息全部发送一个EOF结束标志
	for c := range conn.sendBufs {
		c.close()
	}

	// 通知所有待接收的接收者，已经退出了
	for _, c := range conn.recvBufs {
		c.Close()
	}

	conn.nc.Close()
	conn.isClosed = true
}

func (conn *Conn) read(p []byte) (n int, err error) {
	for n < len(p) {
		conn.Lock()
		if conn.isClosed {
			conn.Unlock()
			return n, io.EOF
		}
		conn.Unlock()
		conn.nc.SetReadDeadline(time.Now().Add(time.Second))
		s, err := conn.nc.Read(p[n:])
		n += s
		if err != nil {
			if e, ok := err.(*net.OpError); ok && e.Timeout() {
				err = nil
				continue
			} else if strings.Contains(err.Error(), "use of closed network connection") {
				err = io.EOF
			}
			return n, err
		}
	}

	return n, nil
}

func (conn *Conn) readMessages() {
	defer func() {
		conn.Done()
		conn.Close()
	}()

	header := make([]byte, HeaderSize)
	keyBuf := make([]byte, MaxKeySize)
	data := make([]byte, MaxDataSize)

	for {
		conn.Lock()
		if conn.isClosed {
			conn.Unlock()
			return
		}
		conn.Unlock()

		if _, err := conn.read(header); err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("[INFO] conn closed")
			} else {
				fmt.Printf("[WARNING] read err from conn: %v\n", err)
			}
			return
		}

		if string(header[:VersionIndex]) != string(ProtocolTag[:]) {
			fmt.Printf("[WARNING] protocal tag err: %v\n", string(header[:VersionIndex]))
		}

		// ignore version

		var (
			keySize  = int(header[KeySizeIndex])<<24 + int(header[KeySizeIndex+1])<<16 + int(header[KeySizeIndex+2])<<8 + int(header[KeySizeIndex+3])
			dataSize = int(header[DataSizeIndex])<<24 + int(header[DataSizeIndex+1])<<16 + int(header[DataSizeIndex+2])<<8 + int(header[DataSizeIndex+3])
			key      string
		)

		if keySize > 0 {
			//	read key
			if _, err := conn.read(keyBuf[:keySize]); err != nil {
				fmt.Printf("[WARNING] read key err: %v\n", string(header[:VersionIndex]))
			}
			key = string(keyBuf[:keySize])
		}

		w := conn.recvBufs[key]
		if w == nil {
			w = newReceiver(key)
			conn.recvBufs[key] = w

			// 读到key对应的消息体时将放入读缓冲中，则链接可以被多个不同key的逻辑复用，
			// 但根据测试用例1，必须要等所有数据读取完成后才能放入读缓冲，因为写端可能还没有将该key和checksum放入共享map中导致断言失败
			//conn.rlock.Lock()
			//conn.rs = append(conn.rs, w)
			//conn.rcond.Signal()
			//conn.rlock.Unlock()
		}

		if dataSize > 0 {
			// read data and write to buffer
			n, err := conn.read(data[:dataSize])
			if err != nil {
				fmt.Printf("[WARNING] read data err: %v\n", err)
			}
			if n < dataSize {
				dataSize = 0
			}
		}
		if dataSize > 0 {
			w.Write(data[:dataSize])

			if w.log != nil {
				w.log.Write(data[:dataSize])
			}
			select {
			case w.notify <- struct{}{}:
			default:
			}
		} else {
			// 规定只有消息头而没有数据的分片为消息结束标志
			// 消息所有数据读取完成了，将该消息放入读缓冲区，等待被读取
			w.Close()
			conn.rlock.Lock()
			conn.rs = append(conn.rs, w)
			conn.rcond.Signal()
			conn.rlock.Unlock()
			// 已经读完的消息接收者，从待接收列表移除
			delete(conn.recvBufs, key)
			if w.log != nil {
				w.log.Close()
			}
		}
	}
}

// NewConn 从一个 TCP 连接得到一个你实现的连接对象
func NewConn(conn net.Conn) *Conn {
	c := &Conn{
		nc:        conn,
		sendBufs:  make(map[*sender]bool),
		recvBufs:  make(map[string]*receiver),
		rlock:     &sync.Mutex{},
		Mutex:     &sync.Mutex{},
		WaitGroup: &sync.WaitGroup{},
	}

	c.rcond = sync.NewCond(c.rlock)
	c.Add(1)

	go c.readMessages()

	return c
}

// 并发读和并发写
func testCase2() {
	var (
		mapKeyToChecksum = map[string]string{}
		lock             sync.Mutex
	)
	ln := startServer(func(conn *Conn) {
		// 服务端向客户端连续进行 2 次传输
		wg := &sync.WaitGroup{}
		for _, key := range []string{newRandomKey(), newRandomKey()} {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				writer, err := conn.Send(key)
				if err != nil {
					panic(err)
				}
				checksum := writeRandomData(writer, sha256.New())
				lock.Lock()
				mapKeyToChecksum[key] = checksum
				lock.Unlock()
				err = writer.Close() //表明该 key 的所有数据已传输完毕
				if err != nil {
					panic(err)
				}
			}(key)
		}
		wg.Wait()
		conn.Close()
	})
	//goland:noinspection GoUnhandledErrorResult
	defer ln.Close()

	conn := dial(ln.Addr().String())
	// 客户端等待服务端的多次传输
	keyCount := 0
	wg := &sync.WaitGroup{}
	for {
		key, reader, err := conn.Receive()
		if err == io.EOF {
			// 服务端所有的数据均传输完毕，关闭连接
			break
		}
		if err != nil {
			panic(err)
		}

		wg.Add(1)
		go func(key string, reader io.Reader) {
			defer wg.Done()
			_checksum := readRandomData(reader, sha256.New())
			lock.Lock()
			checksum, keyExist := mapKeyToChecksum[key]
			lock.Unlock()
			if !keyExist {
				panic(fmt.Sprintln(key, "not exist"))
			}
			assertEqual(_checksum, checksum)
			lock.Lock()
			keyCount++
			lock.Unlock()
		}(key, reader)
	}
	wg.Wait()
	conn.Close()
	assertEqual(keyCount, 2)
}

// 除了上面规定的接口，你还可以自行定义新的类型，变量和函数以满足实现需求

//////////////////////////////////////////////
///////// 接下来的代码为测试代码，请勿修改 /////////
//////////////////////////////////////////////

// 连接到测试服务器，获得一个你实现的连接对象
func dial(serverAddr string) *Conn {
	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		panic(err)
	}
	return NewConn(conn)
}

// 启动测试服务器
func startServer(handle func(*Conn)) net.Listener {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Println("[WARNING] ln.Accept", err)
				return
			}
			go handle(NewConn(conn))
		}
	}()
	return ln
}

// 简单断言
func assertEqual[T comparable](actual T, expected T) {
	if actual != expected {
		panic(fmt.Sprintf("actual:%v expected:%v\n", actual, expected))
	}
}

// 简单 case：单连接，双向传输少量数据
func testCase0() {
	const (
		key  = "Bible"
		data = `Then I heard the voice of the Lord saying, “Whom shall I send? And who will go for us?”
And I said, “Here am I. Send me!”
Isaiah 6:8`
	)
	ln := startServer(func(conn *Conn) {
		// 服务端等待客户端进行传输
		_key, reader, err := conn.Receive()
		if err != nil {
			panic(err)
		}
		assertEqual(_key, key)
		dataB, err := io.ReadAll(reader)
		if err != nil {
			panic(err)
		}
		assertEqual(string(dataB), data)

		// 服务端向客户端进行传输
		writer, err := conn.Send(key)
		if err != nil {
			panic(err)
		}
		n, err := writer.Write([]byte(data))
		if err != nil {
			panic(err)
		}
		if n != len(data) {
			panic(n)
		}
		conn.Close()
	})
	//goland:noinspection GoUnhandledErrorResult
	defer ln.Close()

	conn := dial(ln.Addr().String())
	// 客户端向服务端传输
	writer, err := conn.Send(key)
	if err != nil {
		panic(err)
	}
	n, err := writer.Write([]byte(data))
	if n != len(data) {
		panic(n)
	}
	err = writer.Close()
	if err != nil {
		panic(err)
	}
	// 客户端等待服务端传输
	_key, reader, err := conn.Receive()
	if err != nil {
		panic(err)
	}
	assertEqual(_key, key)
	dataB, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	assertEqual(string(dataB), data)
	conn.Close()
}

// 生成一个随机 key
func newRandomKey() string {
	buf := make([]byte, 8)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(buf)
}

// 读取随机数据，并返回随机数据的校验和：用于验证数据是否完整传输
func readRandomData(reader io.Reader, hash hash.Hash) (checksum string) {
	hash.Reset()
	var buf = make([]byte, 23<<20) //调用者读取时的 buf 大小不是固定的，你的实现中不可假定 buf 为固定值
	for {
		n, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		_, err = hash.Write(buf[:n])
		if err != nil {
			panic(err)
		}
	}
	checksum = hex.EncodeToString(hash.Sum(nil))
	return checksum
}

// 写入随机数据，并返回随机数据的校验和：用于验证数据是否完整传输
func writeRandomData(writer io.Writer, hash hash.Hash) (checksum string) {
	hash.Reset()
	const (
		dataSize = 500 << 20 //一个 key 对应 500MB 随机二进制数据，dataSize 也可以是其他值，你的实现中不可假定 dataSize 为固定值
		bufSize  = 1 << 20   //调用者写入时的 buf 大小不是固定的，你的实现中不可假定 buf 为固定值
	)
	var (
		buf  = make([]byte, bufSize)
		size = 0
	)
	for i := 0; i < dataSize/bufSize; i++ {
		_, err := rand.Read(buf)
		if err != nil {
			panic(err)
		}
		_, err = hash.Write(buf)
		if err != nil {
			panic(err)
		}
		n, err := writer.Write(buf)
		if err != nil {
			panic(err)
		}
		size += n
	}
	if size != dataSize {
		panic(size)
	}
	checksum = hex.EncodeToString(hash.Sum(nil))
	return checksum
}

// 复杂 case：多连接，双向传输，大量数据，多个不同的 key
func testCase1() {
	var (
		mapKeyToChecksum = map[string]string{}
		lock             sync.Mutex
	)
	ln := startServer(func(conn *Conn) {
		// 服务端等待客户端进行传输
		key, reader, err := conn.Receive()
		if err != nil {
			panic(err)
		}
		var (
			h         = sha256.New()
			_checksum = readRandomData(reader, h)
		)
		lock.Lock()
		checksum, keyExist := mapKeyToChecksum[key]
		lock.Unlock()
		if !keyExist {
			panic(fmt.Sprintln(key, "not exist"))
		}
		assertEqual(_checksum, checksum)

		// 服务端向客户端连续进行 2 次传输
		for _, key := range []string{newRandomKey(), newRandomKey()} {
			writer, err := conn.Send(key)
			if err != nil {
				panic(err)
			}
			checksum := writeRandomData(writer, h)
			lock.Lock()
			mapKeyToChecksum[key] = checksum
			lock.Unlock()
			err = writer.Close() //表明该 key 的所有数据已传输完毕
			if err != nil {
				panic(err)
			}
		}
		conn.Close()
	})
	//goland:noinspection GoUnhandledErrorResult
	defer ln.Close()

	conn := dial(ln.Addr().String())
	// 客户端向服务端传输
	var (
		key = newRandomKey()
		h   = sha256.New()
	)
	writer, err := conn.Send(key)
	if err != nil {
		panic(err)
	}
	checksum := writeRandomData(writer, h)
	lock.Lock()
	mapKeyToChecksum[key] = checksum
	lock.Unlock()
	err = writer.Close()
	if err != nil {
		panic(err)
	}

	// 客户端等待服务端的多次传输
	keyCount := 0
	for {
		key, reader, err := conn.Receive()
		if err == io.EOF {
			// 服务端所有的数据均传输完毕，关闭连接
			break
		}
		if err != nil {
			panic(err)
		}
		_checksum := readRandomData(reader, h)
		lock.Lock()
		checksum, keyExist := mapKeyToChecksum[key]
		lock.Unlock()
		if !keyExist {
			panic(fmt.Sprintln(key, "not exist"))
		}
		assertEqual(_checksum, checksum)
		keyCount++
	}
	assertEqual(keyCount, 2)
	conn.Close()
}

func main() {
	testCase0()
	testCase1()
	// 并发读写的测试用例
	testCase2()
}

