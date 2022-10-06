package main

import (
	"bytes"
	"encoding/binary"
	"math"
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

func weightedScore(child Node, straw int64, input int64, round int64) int64 {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.LittleEndian, input)
	if err != nil {
		panic(err)
	}
	err = binary.Write(&buf, binary.LittleEndian, round)
	if err != nil {
		panic(err)
	}
	_, err = buf.WriteString(child.GetId())
	if err != nil {
		panic(err)
	}
	hash := btoi(digestBytes(buf.Bytes()))
	hash = hash & 0xFFFF
	var weightedScore = hash * straw
	return weightedScore
}
