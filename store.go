package main

import (
	"fmt"
	"strconv"

	"github.com/boltdb/bolt"
	uuid "github.com/satori/go.uuid"
)

type notificationStore interface {
	append([]byte) error
	get(generationID string, fromIndex uint64) (*NotificationsResponse, error)
	close() error
}

type boltStore struct {
	db           *bolt.DB
	generationID string
}

func newBoltStore(path string) (*boltStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	store := &boltStore{
		db: db,
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketNotifications))
		if err != nil {
			return fmt.Errorf("error creating notifications bucket: %v", err)
		}

		b, err := tx.CreateBucketIfNotExists([]byte(bucketMetadata))
		if err != nil {
			return fmt.Errorf("error creating metadata bucket: %v", err)
		}
		genID := b.Get([]byte(keyGenerationID))
		if genID == nil {
			genID = []byte(uuid.NewV4().String())
			if err := b.Put([]byte(keyGenerationID), genID); err != nil {
				return fmt.Errorf("error initializing generation ID: %v", err)
			}
		}
		store.generationID = string(genID)
		return nil
	})

	if err != nil {
		return nil, err
	}

	return store, nil
}

func keyFromIndex(index uint64) []byte {
	return []byte(strconv.FormatUint(index, 10))
}

func (bs *boltStore) append(notification []byte) error {
	return bs.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNotifications))
		idx, err := b.NextSequence()
		if err != nil {
			return fmt.Errorf("error getting next sequence number: %v", err)
		}
		if err := b.Put(keyFromIndex(idx), notification); err != nil {
			return fmt.Errorf("error appending notification: %v", err)
		}
		return nil
	})
}

func (bs *boltStore) get(generationID string, fromIndex uint64) (*NotificationsResponse, error) {
	ns := []Notification{}
	err := bs.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(bucketNotifications)).Cursor()

		var k, v []byte
		if generationID == bs.generationID {
			k, v = c.Seek(keyFromIndex(fromIndex))
		} else {
			k, v = c.First()
		}

		for ; k != nil; k, v = c.Next() {
			idx, err := strconv.ParseUint(string(k), 10, 64)
			if err != nil {
				return fmt.Errorf("error parsing index from key: %v", err)
			}

			ns = append(ns, Notification{
				Index: idx,
				Data:  JSONString(v),
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &NotificationsResponse{
		GenerationID:  bs.generationID,
		Notifications: ns,
	}, nil
}

func (bs *boltStore) close() error {
	return bs.db.Close()
}
