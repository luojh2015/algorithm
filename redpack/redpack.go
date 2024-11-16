package main

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	MaxAmount int = 200 * 100
	MinAmount     = 1
)

var (
	rd = rand.New(rand.NewSource(time.Now().Unix()))
)

type redPack struct {
	Amount int
	Number int
}

func NewRedPack(amount int, number int) (*redPack, error) {
	if number < 1 {
		return nil, fmt.Errorf("number error")
	}
	if amount < MinAmount*number || amount > MaxAmount*number {
		return nil, fmt.Errorf("amount error")
	}
	return &redPack{
		Amount: amount,
		Number: number,
	}, nil
}

func (rp *redPack) calcArr() []int {
	remain := rp.Amount
	arr := make([]int, 0, rp.Number)

	for i := 0; i < rp.Number-1; i++ {
		// 剩余金额刚好以最低金额平分，直接返回
		if remain == MinAmount*(rp.Number-i) {
			for ; i < rp.Number; i++ {
				arr = append(arr, MinAmount)
			}
			return arr
		}
		n := rp.getOne(remain, i)
		arr = append(arr, n)
		remain -= n
	}
	arr = append(arr, remain)

	return arr
}

func (rp *redPack) getOne(remain int, i int) int {
	n := MinAmount + rd.Intn(2*remain/(rp.Number-i))
	d := (remain - n) - MinAmount*(rp.Number-i-1)
	if d < 0 {
		n += d
	}
	return n
}

func main() {
	arr := [][2]int{
		{1, 1},
		{2, 2},
		{3, 3},
		{11, 10},
		{10, 5},
		{100, 7},
		{10000, 9},
	}

	for i, v := range arr {
		rp, _ := NewRedPack(v[0], v[1])
		if rp != nil {
			vs := rp.calcArr()
			fmt.Println(i, ": ", vs)
			if len(vs) != rp.Number {
				panic("arr len not eq number")
			}

			s := 0
			for j := range vs {
				if vs[j] < MinAmount {
					panic("get one < MinAmount")
				}
				s += vs[j]
			}

			if s != rp.Amount {
				panic("total not eq amount")
			}
		}
	}
}

