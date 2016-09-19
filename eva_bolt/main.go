package main

import (
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/vipally/cmdline"
	_ "github.com/vipally/cpright"
)

var (
	num_write   int64 = 0
	num_read    int64 = 0
	start_write int64 = 0
	start_read  int64 = 0
	report      int64 = 1
)

const (
	default_report = 1000000
)

func main() {
	cmdline.Int64Var(&num_write, "nw", "num_write", 0, false, "")
	cmdline.Int64Var(&num_read, "nr", "num_read", 0, false, "")
	cmdline.Int64Var(&start_write, "sw", "start_write", -1, false, "")
	cmdline.Int64Var(&start_read, "sr", "start_read", -1, false, "")
	cmdline.Int64Var(&report, "rp", "report", default_report, false, "")
	cmdline.Parse()

	if report <= 0 {
		report = default_report
	}
	db, _ := bolt.Open("./db/bolt.db", 0777, nil)
	defer db.Close()

	db.Batch(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("bucket1"))

		start := time.Now()
		fmt.Println("\n===begin", num_write, num_read, start_write, start_read, report, start)
		last := start
		one := int64(1)
		if num_write > 0 {

			if -1 == start_write {
				start_write = (start.Unix()*984947 + 984847) % 2100000000
			}
			fmt.Println("write start", num_write, start_write, start)
			last = start

			var w_cnt int64 = 0
			for i := one; i <= num_write; i++ {
				n := i + start_write
				r := (uint64(n)*983209+984947)*985723 + 984847
				k := r % 210000000
				key := fmt.Sprintf("key%010d", k)
				val := fmt.Sprintf("val%010d", k)
				if e := b.Put([]byte(key), []byte(val)); e == nil {
					w_cnt++
				}

				if i%report == 0 {
					e := time.Now()
					fmt.Println(i, "/", num_write, "finished", e, e.Sub(start), e.Sub(last))
					last = e
					//tx.Commit()
				}
			}
			end := time.Now()
			fmt.Println("write end", w_cnt, "/", num_write, end, end.Sub(start), end.Sub(last))
		}

		if num_read > 0 {
			start_r := time.Now()
			if -1 == start_read {
				start_read = (start_r.Unix()*984947 + 984847) % 2100000000
			}
			last = start_r
			fmt.Println("read start", num_read, start_read, start_r)
			var r_cnt int64 = 0
			for i := one; i <= num_read; i++ {
				n := i + start_read
				r := (uint64(n)*983209+984947)*985723 + 984847
				k := r % 210000000

				r2 := (uint64(k)*999983+994027)*995539 + 994867
				k2 := r2 % 210000000
				key2 := fmt.Sprintf("key%010d", k2)
				if e := b.Get([]byte(key2)); e == nil {
					//fmt.Println(key2, ": ", string(v))
					r_cnt++
				} else {
					//fmt.Println(e)
				}

				if i%report == 0 {
					e := time.Now()
					fmt.Println(i, "/", num_read, "finished", e, e.Sub(start), e.Sub(last))
					last = e
				}
			}
			end_r := time.Now()
			fmt.Println("read end", r_cnt, "/", num_read, end_r, end_r.Sub(start), end_r.Sub(last))
		}

		//tx.Commit()
		return nil
	})
}

//func main() {
//	cmdline.Parse()

//	db, err := bolt.Open("./db/bolt.db", 0600, nil)
//	if err != nil {
//		panic(err)
//	}
//	defer db.Close()

//	db.Batch(func(tx *bolt.Tx) error {
//		b, err := tx.CreateBucketIfNotExists([]byte("bucket1"))
//		if err != nil {
//			return err
//		}

//		// Set the value "bar" for the key "foo".
//		if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
//			return err
//		}
//		//tx.Commit()
//		return nil
//	})
//	//fmt.Println(db.String())
//}
