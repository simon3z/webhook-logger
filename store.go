package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/boltdb/bolt"
	uuid "github.com/satori/go.uuid"
)

const (
	bucketMetadata      = "metadata"
	bucketNotifications = "notifications"

	keyGenerationID = "generationID"
)

type notificationStore interface {
	append(topic string, data interface{}) error
	get(topic string, generationID string, fromIndex uint64) (*NotificationsResponse, error)
	close() error
}

type boltStore struct {
	db           *bolt.DB
	generationID string
	options      *boltStoreOptions

	stop chan struct{}
	done chan struct{}
}

type boltStoreOptions struct {
	retention  time.Duration
	gcInterval time.Duration
	path       string
}

func newBoltStore(opts *boltStoreOptions) (*boltStore, error) {
	db, err := bolt.Open(opts.path, 0600, nil)
	if err != nil {
		return nil, err
	}

	store := &boltStore{
		db:      db,
		options: opts,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
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

func (bs *boltStore) start() {
	gcTicker := time.NewTicker(bs.options.gcInterval)
	for {
		select {
		case <-bs.stop:
			close(bs.done)
		case <-gcTicker.C:
			log.Println("Running GC cycle to remove old entries...")
			num, err := bs.gc(time.Now().Add(-bs.options.retention))
			if err != nil {
				log.Println("Error running GC cycle:", err)
			} else {
				log.Printf("Deleted %d old entries", num)
			}
		}
	}
}

func keyFromIndex(index uint64) []byte {
	return []byte(strconv.FormatUint(index, 10))
}

func (bs *boltStore) append(topic string, data interface{}) error {
	return bs.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucketNotifications))
		b, err := root.CreateBucketIfNotExists([]byte(topic))
		if err != nil {
			return fmt.Errorf("error creating bucket for topic %q: %v", topic, err)
		}
		idx, err := b.NextSequence()
		if err != nil {
			return fmt.Errorf("error getting next sequence number: %v", err)
		}

		n := Notification{
			Index:     idx,
			Timestamp: time.Now(),
			Data:      data,
		}
		buf, err := json.Marshal(n)
		if err != nil {
			return fmt.Errorf("error marshalling notification: %v", err)
		}
		if err := b.Put(keyFromIndex(idx), buf); err != nil {
			return fmt.Errorf("error appending notification: %v", err)
		}
		return nil
	})
}

func (bs *boltStore) get(topic string, generationID string, fromIndex uint64) (*NotificationsResponse, error) {
	ns := []Notification{}
	err := bs.db.View(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucketNotifications))
		b := root.Bucket([]byte(topic))
		if b == nil {
			// Topic doesn't exist yet, return it as an empty set.
			return nil
		}
		c := b.Cursor()

		var k, v []byte
		if generationID == bs.generationID {
			k, v = c.Seek(keyFromIndex(fromIndex))
		} else {
			k, v = c.First()
		}

		var n Notification
		for ; k != nil; k, v = c.Next() {
			if err := json.Unmarshal(v, &n); err != nil {
				return fmt.Errorf("unable to unmarshal notification: %v", err)
			}

			ns = append(ns, n)
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

func (bs *boltStore) gc(olderThan time.Time) (int, error) {
	var numDeleted int
	return numDeleted, bs.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket([]byte(bucketNotifications))
		rootC := root.Cursor()

		for topic, _ := rootC.First(); topic != nil; topic, _ = rootC.Next() {
			c := root.Bucket(topic).Cursor()

			// For now, this goes through all entries and doesn't abort after the first
			// encountered entry that should be kept, just in case there are time/date
			// glitches on a machine and timestamps end up being out of order.
			//
			// TODO: Possibly reconsider this for performance reasons if the DB gets huge.
			var n Notification
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if err := json.Unmarshal(v, &n); err != nil {
					return fmt.Errorf("unable to unmarshal notification: %v", err)
				}

				if n.Timestamp.Before(olderThan) {
					if err := c.Delete(); err != nil {
						return fmt.Errorf("unable to delete notification: %v", err)
					}
					numDeleted++
				}
			}
		}
		return nil
	})
}

func (bs *boltStore) close() error {
	close(bs.stop)
	<-bs.done
	return bs.db.Close()
}
