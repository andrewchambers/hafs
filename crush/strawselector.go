package main

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/cespare/xxhash/v2"
)

type StrawSelector struct {
	Straws map[Node]int64
}

func NewStrawSelector(nodes []Node) *StrawSelector {
	s := &StrawSelector{
		Straws: make(map[Node]int64),
	}
	numLeft := len(nodes)
	var straw float64 = 1.0
	var wbelow float64 = 0.0
	var lastw float64 = 0.0
	i := 0
	for i < len(nodes) {
		current := nodes[i]
		if current.GetWeight() == 0 {
			s.Straws[current] = 0
			i += 1
			continue
		}
		s.Straws[current] = int64(straw * 0x10000)
		i += 1
		if i == len(nodes) {
			break
		}

		current = nodes[i]
		previous := nodes[i-1]
		if current.GetWeight() == previous.GetWeight() {
			continue
		}
		wbelow += (float64(previous.GetWeight()) - lastw) * float64(numLeft)
		for j := 0; j < len(nodes); j++ {
			if nodes[j].GetWeight() == current.GetWeight() {
				numLeft -= 1
			} else {
				break
			}
		}
		var wnext float64 = float64(int64(numLeft) * (current.GetWeight() - previous.GetWeight()))
		pbelow := wbelow / (wbelow + wnext)
		straw *= math.Pow(1.0/pbelow, 1.0/float64(numLeft))
		lastw = float64(previous.GetWeight())
	}
	return s
}

func weightedScore(child Node, straw int64, input int64, round int64) int64 {

	digest := xxhash.Digest{}

	err := binary.Write(&digest, binary.LittleEndian, input)
	if err != nil {
		panic(err)
	}
	err = binary.Write(&digest, binary.LittleEndian, round)
	if err != nil {
		panic(err)
	}
	_, err = io.WriteString(&digest, child.GetId())
	if err != nil {
		panic(err)
	}
	hash := int64(digest.Sum64())
	hash = hash & 0xFFFF
	var weightedScore = hash * straw
	return weightedScore
}

func (s *StrawSelector) Select(input int64, round int64) Node {
	var result Node
	var hiScore = int64(-1)
	for child, straw := range s.Straws {
		var score = weightedScore(child, straw, input, round)
		if score > hiScore {
			result = child
			hiScore = score
		}
	}
	if result == nil {
		panic("Illegal state")
	}
	return result
}
