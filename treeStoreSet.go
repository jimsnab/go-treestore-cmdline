package treestore_cmdline

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jimsnab/go-lane"
	"github.com/jimsnab/go-treestore"
)

type (
	treeStoreSet struct {
		mu         sync.Mutex
		appVersion int
		basePath   string
		dbs        map[string]*treestore.TreeStore
		users      map[string]*treeStoreUser
		dirty      atomic.Int32
	}
)

func newTreeStoreSet(l lane.Lane, basePath string, appVersion int) (tss *treeStoreSet, err error) {
	tss = &treeStoreSet{
		basePath:   basePath,
		appVersion: appVersion,
		dbs:        map[string]*treestore.TreeStore{},
		users:      map[string]*treeStoreUser{"default": newTreeStoreUser()},
	}

	tss.createDbUnlocked(l, "main")
	if basePath != "" {
		l.Tracef("loading database(s) from base path %s", basePath)

		// Search the file system for persisted data, and load each data store
		dir, fileBase := filepath.Split(basePath)
		if dir == "" {
			dir = "."
		}

		// data store files are <base-name>.<name>.db where <base-name> is user provided
		// and <name> is the data store index.
		err = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if !d.IsDir() {
				if strings.HasPrefix(d.Name(), fileBase) && strings.HasSuffix(d.Name(), ".db") {
					name := d.Name()[len(fileBase):]
					name = strings.Trim(name, ".db")
					if len(name) > 0 {
						// found a data store file - load it
						ts, _ := tss.createDbUnlocked(l, name)
						l.Tracef("loading database %s from %s", name, path)
						loadErr := ts.Load(l, path)
						if loadErr != nil {
							l.Errorf("error loading %s: %s", path, err.Error())
							return loadErr
						}
					}
				}
			}

			return nil
		})

		if err != nil {
			tss = nil
			return
		}
	}

	return
}

func (tss *treeStoreSet) save(l lane.Lane) error {
	if tss.dirty.Swap(0) > 0 {
		l.Trace("saving treestore set")
		for index, ts := range tss.dbs {
			filename := tss.treeStoreFileName(index)
			l.Tracef("saving %s to %s", index, filename)
			err := ts.Save(l, filename)
			if err != nil {
				l.Errorf("failed to save %s to %s: %s", index, filename, err.Error())
				return err
			}
		}
	}
	return nil
}

func (tss *treeStoreSet) treeStoreFileName(index string) string {
	if tss.basePath == "" {
		return ""
	}
	return fmt.Sprintf("%s.%s.db", tss.basePath, index)
}

func (tss *treeStoreSet) createDbUnlocked(l lane.Lane, index string) (ts *treestore.TreeStore, valid bool) {
	ts, exists := tss.dbs[index]
	if !exists {
		ts = treestore.NewTreeStore(l.Derive(), tss.appVersion)
		tss.dbs[index] = ts
	}

	return ts, true
}

func (tss *treeStoreSet) getDb(l lane.Lane, index string, create bool) (ts *treestore.TreeStore, valid bool) {
	tss.mu.Lock()
	defer tss.mu.Unlock()

	ts, exists := tss.dbs[index]
	if !exists {
		if create {
			if ts, valid = tss.createDbUnlocked(l, index); !valid {
				return
			}
		} else {
			return
		}
	}

	valid = true
	return
}

func (tss *treeStoreSet) discardDb(index string) {
	tss.mu.Lock()
	defer tss.mu.Unlock()

	delete(tss.dbs, index)
}

func (tss *treeStoreSet) discardAll() {
	tss.mu.Lock()
	defer tss.mu.Unlock()

	tss.dbs = map[string]*treestore.TreeStore{}
}

func (tss *treeStoreSet) getUser(userName string) (tsu *treeStoreUser, exists bool) {
	tsu, exists = tss.users[userName]
	return
}
