package main

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/google/shlex"
	"github.com/xlab/treeprint"
)

type Comparitor func(Node) bool

type Selector interface {
	Select(input int64, round int64) Node
}

type Node interface {
	GetId() string
	GetTypeIdx() int
	GetChildren() []Node
	GetWeight() int64
	IsFailed() bool
	GetParent() Node
	IsLeaf() bool
	Select(input int64, round int64) Node
}

type Location []string

func (l Location) Equals(other Location) bool {
	if len(l) != len(other) {
		return false
	}
	// Reverse order should exit faster since most
	// locations actually have a unique end.
	for i := len(l) - 1; i >= 0; i -= 1 {
		if l[i] != other[i] {
			return false
		}
	}
	return true
}

func (l Location) String() string {
	return fmt.Sprintf("%v", []string(l))
}

type StorageNodeInfo struct {
	Location   Location
	Failed     bool
	UsedSpace  int64
	TotalSpace int64
}

type StorageNode struct {
	StorageNodeInfo
	Id      string
	TypeIdx int
	Parent  Node
}

func (n *StorageNode) GetChildren() []Node {
	return []Node{}
}

func (n *StorageNode) IsFailed() bool {
	return n.Failed
}

func (n *StorageNode) GetWeight() int64 {
	return n.TotalSpace
}

func (n *StorageNode) GetTypeIdx() int {
	return n.TypeIdx
}

func (n *StorageNode) GetId() string {
	return n.Id
}

func (n *StorageNode) GetParent() Node {
	return n.Parent
}

func (n *StorageNode) IsLeaf() bool {
	return true
}

func (n *StorageNode) Select(input int64, round int64) Node {
	return nil
}

type InternalNode struct {
	Id          string
	TypeIdx     int
	Parent      Node
	Children    []Node
	ChildNames  []string
	NameToChild map[string]Node
	Failed      bool
	UsedSpace   int64
	TotalSpace  int64
	Selector    Selector
}

func (n *InternalNode) GetChildren() []Node {
	return n.Children
}

func (n *InternalNode) IsFailed() bool {
	return n.Failed
}

func (n *InternalNode) GetWeight() int64 {
	return n.TotalSpace
}

func (n *InternalNode) GetId() string {
	return n.Id
}

func (n *InternalNode) GetTypeIdx() int {
	return n.TypeIdx
}

func (n *InternalNode) GetParent() Node {
	return n.Parent
}

func (n *InternalNode) IsLeaf() bool {
	return len(n.Children) == 0
}

func (n *InternalNode) Select(input int64, round int64) Node {
	return n.Selector.Select(input, round)
}

type StorageHierarchy struct {
	Types     []string
	TypeToIdx map[string]int
	IdxToType map[int]string
	Root      *InternalNode
}

func NewStorageHierarchyFromSpec(s string) (*StorageHierarchy, error) {

	types, err := shlex.Split(s)
	if err != nil {
		return nil, fmt.Errorf("unable to split types: %s", err)
	}

	h := &StorageHierarchy{
		Types:     types,
		TypeToIdx: make(map[string]int),
		IdxToType: make(map[int]string),
		Root: &InternalNode{
			NameToChild: make(map[string]Node),
		},
	}
	for i, t := range h.Types {
		if _, ok := h.TypeToIdx[t]; ok {
			return nil, fmt.Errorf("duplicate type %s in spec", t)
		}
		h.TypeToIdx[t] = i
		h.IdxToType[i] = t
	}
	return h, nil
}

func (h *StorageHierarchy) ContainsStorageNodeAtLocation(location Location) bool {
	if len(h.Types) != len(location) {
		return false
	}
	n := h.Root
	for _, name := range location {
		child, ok := n.NameToChild[name]
		if !ok {
			return false
		}
		switch child := child.(type) {
		case *StorageNode:
			return true
		case *InternalNode:
			n = child
		}
	}
	return false
}

func (h *StorageHierarchy) AddStorageNode(ni *StorageNodeInfo) error {

	n := &StorageNode{
		StorageNodeInfo: *ni,
	}

	// Validate compatible.
	if len(n.Location) != len(h.Types) {
		return fmt.Errorf(
			"location %v is not compatible with type schema %v, expected %d members",
			n.Location,
			h.Types,
			len(h.Types),
		)
	}

	idBuf := bytes.Buffer{}

	node := h.Root
	for i, loc := range n.Location {
		ty := h.Types[i]

		_, err := idBuf.WriteString(loc)
		if err != nil {
			panic(err)
		}
		if i != 0 {
			_, err := idBuf.WriteString(" ")
			if err != nil {
				panic(err)
			}
		}

		if i != len(n.Location)-1 {
			if _, ok := node.NameToChild[loc]; !ok {
				node.NameToChild[loc] = &InternalNode{
					Id:          idBuf.String(),
					TypeIdx:     h.TypeToIdx[ty],
					NameToChild: make(map[string]Node),
				}
			}
			node = node.NameToChild[loc].(*InternalNode)
		} else {
			if _, ok := node.NameToChild[loc]; ok {
				return fmt.Errorf(
					"location '%s' has already been added to storage heirarchy",
					n.Location,
				)
			}
			n.Id = idBuf.String()
			n.TypeIdx = h.TypeToIdx[ty]
			node.NameToChild[loc] = n
		}
	}

	return nil
}

func (h *StorageHierarchy) Finish() {
	var recurse func(n *InternalNode)
	recurse = func(n *InternalNode) {
		n.ChildNames = make([]string, 0, len(n.NameToChild))
		for name, child := range n.NameToChild {
			n.ChildNames = append(n.ChildNames, name)
			switch child := child.(type) {
			case *InternalNode:
				recurse(child)
				n.UsedSpace += child.UsedSpace
				n.TotalSpace += child.TotalSpace
			case *StorageNode:
				n.UsedSpace += child.UsedSpace
				n.TotalSpace += child.TotalSpace
			}
		}
		sort.Strings(n.ChildNames)
		n.Children = make([]Node, 0, len(n.ChildNames))
		for _, name := range n.ChildNames {
			n.Children = append(n.Children, n.NameToChild[name])
		}

		if len(n.Children) != 0 {
			n.Failed = true
			for _, child := range n.Children {
				if !child.IsFailed() {
					n.Failed = false
				}
			}
		}

		n.Selector = NewHashingSelector(n.Children)
	}
	recurse(h.Root)
}

type CrushSelection struct {
	Type  string
	Count int
}

func (h *StorageHierarchy) Crush(input string, selections []CrushSelection) ([]Location, error) {

	if len(selections) == 0 {
		return nil, errors.New("expected at least one selection rule")
	}

	expectedCount := 1

	nodes := []Node{h.Root}
	hashedInput := btoi(digestString(input))

	for _, sel := range selections {
		nodeType, ok := h.TypeToIdx[sel.Type]
		if !ok {
			return nil, fmt.Errorf("unable to do CRUSH mapping, unknown type '%s'", sel.Type)
		}

		expectedCount *= sel.Count
		nextNodes := make([]Node, 0, len(nodes)*sel.Count)

		for _, n := range nodes {
			selection := h.doSelect(n, hashedInput, sel.Count, nodeType, nil)
			nextNodes = append(nextNodes, selection...)
		}
		nodes = nextNodes
	}

	locations := make([]Location, 0, len(nodes))
	for _, node := range nodes {
		storageNode, ok := node.(*StorageNode)
		if !ok {
			return nil, fmt.Errorf("'%s' is a not a storage node", node.GetId())
		}
		locations = append(locations, storageNode.Location)
	}

	if len(locations) != expectedCount {
		return locations, errors.New("unable to satisfy crush placement")
	}

	return locations, nil
}

func (h *StorageHierarchy) doSelect(parent Node, input int64, count int, nodeTypeIdx int, c Comparitor) []Node {
	var results []Node
	//if len(parent.Children) < count {
	//	panic("Asked for more node than are available")
	//}
	var rPrime = int64(0)
	for r := 1; r <= count; r++ {
		var failure = 0
		var loopbacks = 0
		var escape = false
		var retryOrigin = false
		var out Node
		for {
			retryOrigin = false
			var in = parent
			var skip = make(map[Node]bool)
			var retryNode = false
			for {
				retryNode = false
				rPrime = int64(r + failure)
				out = in.Select(input, rPrime)
				if out.GetTypeIdx() != nodeTypeIdx {
					in = out
					retryNode = true
				} else {
					if contains(results, out) {
						if !nodesAvailable(in, results, skip) {
							if loopbacks == 150 {
								escape = true
								break
							}
							loopbacks += 1
							retryOrigin = true
						} else {
							retryNode = true
						}
						failure += 1

					} else if c != nil && !c(out) {
						skip[out] = true
						if !nodesAvailable(in, results, skip) {
							if loopbacks == 150 {
								escape = true
								break
							}
							loopbacks += 1
							retryOrigin = true
						} else {
							retryNode = true
						}
						failure += 1
					} else if isDefunct(out) {
						failure++
						if loopbacks == 150 {
							escape = true
							break
						}
						loopbacks += 1
						retryOrigin = true
					} else {
						break
					}
				}
				if !retryNode {
					break
				}
			}
			if !retryOrigin {
				break
			}
		}
		if escape {
			continue
		}
		results = append(results, out)
	}
	return results
}

func nodesAvailable(parent Node, selected []Node, rejected map[Node]bool) bool {
	var children = parent.GetChildren()
	for _, child := range children {
		if !isDefunct(child) {
			if ok := contains(selected, child); !ok {
				if _, ok := rejected[child]; !ok {
					return true
				}
			}
		}
	}
	return false
}

func contains(s []Node, n Node) bool {
	for _, a := range s {
		if a == n {
			return true
		}
	}
	return false
}

func isDefunct(n Node) bool {
	if n.IsLeaf() && n.IsFailed() {
		return true
	}
	return false
}

func (h *StorageHierarchy) AsciiTree() string {
	t := treeprint.New()
	var recurse func(t treeprint.Tree, n *InternalNode)
	recurse = func(t treeprint.Tree, n *InternalNode) {
		for _, name := range n.ChildNames {
			switch child := n.NameToChild[name].(type) {
			case *InternalNode:
				childT := t.AddBranch(name)
				recurse(childT, child)
			case *StorageNode:
				status := "+"
				if child.Failed {
					status = "!"
				}
				meta := fmt.Sprintf("%d %s", child.TotalSpace, status)
				t.AddBranch(name).SetMetaValue(meta)
			}
		}
	}
	recurse(t, h.Root)
	return t.String()
}
