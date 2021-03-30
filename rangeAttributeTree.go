package contentpubsub

import "fmt"

type bstnode struct {
	value int
	left  *bstnode
	right *bstnode
}

type RangeAttributeTree struct {
	root *bstnode
}

func (b *RangeAttributeTree) reset() {
	b.root = nil
}

func (b *RangeAttributeTree) insert(value int) {
	b.insertRec(b.root, value)
}

func (b *RangeAttributeTree) insertRec(node *bstnode, value int) *bstnode {
	if b.root == nil {
		b.root = &bstnode{
			value: value,
		}
		return b.root
	}
	if node == nil {
		return &bstnode{value: value}
	}
	if value <= node.value {
		node.left = b.insertRec(node.left, value)
	}
	if value > node.value {
		node.right = b.insertRec(node.right, value)
	}
	return node
}

func (b *RangeAttributeTree) find(value int) error {
	node := b.findRec(b.root, value)
	if node == nil {
		return fmt.Errorf("Value: %d not found in tree", value)
	}
	return nil
}

func (b *RangeAttributeTree) findRec(node *bstnode, value int) *bstnode {
	if node == nil {
		return nil
	}
	if node.value == value {
		return b.root
	}
	if value < node.value {
		return b.findRec(node.left, value)
	}
	return b.findRec(node.right, value)
}

func (b *RangeAttributeTree) inorder() {
	b.inorderRec(b.root)
}

func (b *RangeAttributeTree) inorderRec(node *bstnode) {
	if node != nil {
		b.inorderRec(node.left)
		fmt.Println(node.value)
		b.inorderRec(node.right)
	}
}
