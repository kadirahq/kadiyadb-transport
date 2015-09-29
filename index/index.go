package index

// Index stores record IDs for each unique field combination.
type Index struct {
	root *TNode
	logs *Logs
	snap *Snap
}

// NewIndexRO loads an existing index in read-only mode
func NewIndexRO(path string) (i *Index, err error) {
	for {
		// NOTE: Not a loop. Using this to BREAK.
		snap, err := NewSnap(path + "snap_")
		if err != nil {
			break
		}

		root, err := snap.LoadRoot()
		if err != nil {
			snap.Close()
			break
		}

		i = &Index{
			root: root,
			snap: snap,
		}

		return i, nil
	}

	// If we've come to this point, snapshot data doesn't exist or is corrupt
	// Try to load data from log files if available and immediately create a
	// new snapshot which can be used when this index is loaded next time.

	logs, err := NewLogs(path + "log_")
	if err != nil {
		return nil, err
	}

	root, err := logs.Load()
	if err != nil {
		return nil, err
	}

	if err := logs.Close(); err != nil {
		return nil, err
	}

	snap, err := NewSnap(path + "snap_")
	if err != nil {
		return nil, err
	}

	if err := snap.Store(root); err != nil {
		snap.Close()
		return nil, err
	}

	i = &Index{
		root: root,
		snap: snap,
	}

	return i, nil
}

// NewIndexRW loads an existing index in read-write mode
func NewIndexRW(path string) (i *Index, err error) {
	logs, err := NewLogs(path + "log_")
	if err != nil {
		return nil, err
	}

	root, err := logs.Load()
	if err != nil {
		return nil, err
	}

	i = &Index{
		root: root,
		logs: logs,
	}

	return i, nil
}

// Add inserts a new node to the index if it's not available.
// This is not thread safe therefore should be run sequentially.
func (i *Index) Add(fields []string) (node *Node, err error) {
	node = &Node{
		Fields:   fields,
		RecordID: i.logs.nextID,
	}
	tnode := WrapNode(node)

	if err := tnode.Validate(); err != nil {
		return nil, err
	}

	if err := i.logs.Store(tnode); err != nil {
		return nil, err
	}

	i.root.Append(tnode)

	return node, nil
}

// Find finds all existing index nodes with given field pattern.
// The '*' can be used to match any value for the index field.
func (i *Index) Find(fields []string) (ns []*Node, err error) {
	if err := i.ensureBranch(fields); err != nil {
		return nil, err
	}

	return i.root.Find(fields)
}

// FindOne finds the index nodes with exact given field combination.
// `n` is nil if the no nodes exist in the index with given fields.
func (i *Index) FindOne(fields []string) (n *Node, err error) {
	if err := i.ensureBranch(fields); err != nil {
		return nil, err
	}

	return i.root.FindOne(fields)
}

// Close closes the index
func (i *Index) Close() (err error) {
	if i.logs != nil {
		if err := i.logs.Close(); err != nil {
			return err
		}
	}

	if i.snap != nil {
		if err := i.snap.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (i *Index) ensureBranch(fields []string) (err error) {
	// TODO ensure branch is loaded
	return nil
}