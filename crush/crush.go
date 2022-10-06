package main

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

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

type Location [][2]string

func (l Location) String() string {
	var buf bytes.Buffer
	for i, kv := range l {
		sep := ""
		if i != 0 {
			sep = " "
		}
		_, err := fmt.Fprintf(&buf, "%s%s=%s", sep, kv[0], kv[1])
		if err != nil {
			panic(err)
		}
	}
	return buf.String()
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
	h := &StorageHierarchy{
		Types:     strings.Split(s, " "),
		TypeToIdx: make(map[string]int),
		IdxToType: make(map[int]string),
		Root: &InternalNode{
			NameToChild: make(map[string]Node),
		},
	}
	for i, t := range h.Types {
		if strings.Contains(t, "=") {
			return nil, fmt.Errorf("storage type %s cannot contain '='", t)
		}
		if _, ok := h.TypeToIdx[t]; ok {
			return nil, fmt.Errorf("duplicate type %s in spec", t)
		}
		h.TypeToIdx[t] = i
		h.IdxToType[i] = t
	}
	return h, nil
}

func (h *StorageHierarchy) AddStorageNode(ni *StorageNodeInfo) error {

	n := &StorageNode{
		StorageNodeInfo: *ni,
	}

	// Validate compatible.
	for i, kv := range n.Location {
		ty, _ := kv[0], kv[1]
		if h.Types[i] != ty {
			return fmt.Errorf(
				"location '%s' is not compatible with type '%s'",
				n.Location,
				strings.Join(h.Types, " "),
			)
		}
	}

	idBuf := bytes.Buffer{}

	node := h.Root
	for i, kv := range n.Location {
		ty, loc := kv[0], kv[1]

		_, err := idBuf.WriteString(loc)
		if err != nil {
			panic(err)
		}
		if i != 0 {
			_, err := idBuf.WriteString("/")
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
	nodes := []Node{h.Root}

	hashedInput := btoi(digestString(input))

	for _, sel := range selections {
		nodeType, ok := h.TypeToIdx[sel.Type]
		if !ok {
			return nil, fmt.Errorf("unable to do CRUSH mapping, unknown type '%s'", sel.Type)
		}
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
				meta := fmt.Sprintf("%d/%d %s", child.UsedSpace, child.TotalSpace, status)
				t.AddBranch(name).SetMetaValue(meta)
			}
		}
	}
	recurse(t, h.Root)
	return t.String()
}

func main() {
	h, err := NewStorageHierarchyFromSpec("server")
	if err != nil {
		panic(err)
	}

	storageNodes := []StorageNodeInfo{}

	for i := 0; i < 10; i += 1 {
		storageNodes = append(storageNodes,
			StorageNodeInfo{
				Location: Location{
					{"server", fmt.Sprintf("server%d", i)},
				},
				TotalSpace: 100,
			},
		)
	}

	for _, storageNode := range storageNodes {
		err = h.AddStorageNode(&storageNode)
		if err != nil {
			panic(err)
		}
	}

	h.Finish()

	_, _ = fmt.Println(h.AsciiTree())

	if err != nil {
		panic(err)
	}

	selections := []CrushSelection{
		CrushSelection{
			Type:  "server",
			Count: 1,
		},
	}

	counts := make(map[string]int64)

	for i := 0; i < 1000000; i++ {
		k := fmt.Sprintf("%d", i)
		locations, err := h.Crush(k, selections)
		if err != nil {
			panic(err)
		}

		n, _ := counts[locations[0][0][1]]
		counts[locations[0][0][1]] = n + 1
	}

	fmt.Printf("%#v", counts)
}
