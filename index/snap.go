package index

import (
	"path"

	"github.com/kadirahq/go-tools/segmap"
)

const (
	// index file prefix when stored in snapshot format
	// index files will be named "snap_0, snap_1, ..."
	prefixsnap = "snap_"

	// Size of the segment file
	// !IMPORTANT if this value changes, the database will not be able to use
	// older data. To avoid accidental changes, this value is hardcoded here.
	segszsnap = 1024 * 1024 * 20
)

// offset struct contains offset range for a top level branch
// These values point to a position in the snapshot data file
type offset struct {
	from int64
	to   int64
}

// Snap helps create and load index pre-built index trees from snapshot files.
// Index snapshots are read-only, any changes require a rebuild of the snapshot.
type Snap struct {
	RootNode *TNode
	dataFile *segmap.Store
	offsets  map[string]offset
}

// LoadSnap opens an index persister which stores pre-built index trees.
// When loading a index snapshot, only the top level of the tree is loaded.
// All other tree branches are loaded only when it's necessary (on request).
func LoadSnap(dir string) (s *Snap, err error) {
	segpath := path.Join(dir, prefixsnap)
	rf, err := segmap.New(segpath, segszsnap)
	if err != nil {
		return nil, err
	}

	if err := rf.LoadAll(); err != nil {
		return nil, err
	}

	if err := rf.Lock(); err != nil {
		return nil, err
	}

	size := rf.Length() * segszsnap
	root, err := LoadBranch(rf, 0, int64(size))
	if err != nil {
		return nil, err
	}

	if err := rf.Close(); err != nil {
		return nil, err
	}

	df, err := segmap.New(segpath, segszsnap)
	if err != nil {
		return nil, err
	}

	if err := df.LoadAll(); err != nil {
		return nil, err
	}

	s = &Snap{
		RootNode: root,
		dataFile: df,
		offsets:  map[string]offset{},
	}

	return s, nil
}

// StoreSnap creates a snapshot on given path and returns created snapshot.
// This snapshot will have the complete index tree already loaded into ram.
func StoreSnap(dir string, root *TNode) (s *Snap, err error) {
	// ! TODO create snapshot at given dir
	return nil, nil
}

// LoadBranch loads an index tree branch from a segmented store
// This can be used to read the index root or top level branches
func LoadBranch(rf *segmap.Store, from, to int64) (tree *TNode, err error) {
	// ! TODO load tree branch from a snapshot
	return nil, nil
}

// Branch function loads a branch from the data memory map
func (s *Snap) Branch(key string) (tree *TNode, err error) {
	// ! TODO load tree branch from a snapshot
	return nil, nil
}

// Sync syncs the snapshot store
func (s *Snap) Sync() (err error) {
	if err := s.dataFile.Sync(); err != nil {
		return err
	}

	return nil
}

// Close releases resources
func (s *Snap) Close() (err error) {
	if err := s.dataFile.Close(); err != nil {
		return err
	}

	return nil
}
