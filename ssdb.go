package ssdb

import (
	"bytes"
	"fmt"
	"net"
	//"reflect"
	"strconv"
)

type Client struct {
	sock     *net.TCPConn
	recv_buf bytes.Buffer
}

var (
	ErrBadResponse = fmt.Errorf("bad response")
	ErrNotEnoughParams = fmt.Errorf("not enougn params")
)

func Connect(ip string, port int) (*Client, error) {
	addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return nil, err
	}
	sock, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	var c Client
	c.sock = sock
	return &c, nil
}

func (c *Client) Do(args ...interface{}) ([]string, error) {
	err := c.send(args)
	if err != nil {
		return nil, err
	}
	resp, err := c.recv()
	return resp, err
}

// Key-Value
func (c *Client) Set(key, val string) (res bool, err error) {
	resp, err := c.Do("set", key, val)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		res = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) Setx(key, val string, expires int) (res bool, err error) {
	resp, err := c.Do("setx", key, val, expires)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		res = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) Setnx(key, val string) (num int, err error) {
	resp, err := c.Do("setnx", key, val)
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) Get(key string) (val interface{}, err error) {
	resp, err := c.Do("get", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		val = resp[1]
		return
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) GetSet(key, val string) (last interface{}, err error) {
	resp, err := c.Do("getset", key, val)
	if len(resp) == 2 && resp[0] == "ok" {
		last = resp[1]
		return
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) Del(key string) (interface{}, error) {
	resp, err := c.Do("del", key)
	if err != nil {
		return nil, err
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return true, nil
	}
	return nil, ErrBadResponse
}

func (c *Client) Incr(key string, num int) (res int, err error) {
	resp, err := c.Do("incr", key, num)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) Decr(key string, num int) (res int, err error) {
	resp, err := c.Do("decr", key, num)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) MultiSet(kvPair map[string]string) (success interface{}, err error) {
	args := []interface{}{"multi_set"}
	for k, v := range kvPair {
		args = append(args, k, v)
	}
	resp, err := c.Do(args...)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) MultiGet(keys ...string) (kvPair map[string]string, err error) {
	args := []interface{}{"multi_get"}
	for _, k := range keys {
		args = append(args, k)
	}
	resp, err := c.Do(args...)
	if err != nil {
		return
	}
	if len(resp)&1 == 1 && resp[0] == "ok" {
		kvPair = map[string]string{}
		res := resp[1:]
		for i := 0; i < len(res); i += 2 {
			k := res[i]
			v := res[i+1]
			kvPair[k] = v
		}
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) MultiDel(keys ...string) (success interface{}, err error) {
	args := []interface{}{"multi_del"}
	for _, k := range keys {
		args = append(args, k)
	}
	resp, err := c.Do(args...)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) Scan(startKey string, endKey string, limit int) (kvList [][2]string, err error) {
	resp, err := c.Do("scan", startKey, endKey, limit)
	if len(resp)&1 == 1 && resp[0] == "ok" {
		kvList = [][2]string{}
		res := resp[1:]
		for i := 0; i < len(res); i += 2 {
			k := res[i]
			v := res[i+1]
			kv := [2]string{k, v}
			kvList = append(kvList, kv)
		}
		return
	}
	err = ErrBadResponse
	return
}

//Key-Map
func (c *Client) HSet(key, field, val string) (success interface{}, err error) {
	resp, err := c.Do("hset", key, field, val)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) HGet(key, field string) (val interface{}, err error) {
	resp, err := c.Do("hget", key, field)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		val = resp[1]
		return
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) HDel(key, field string) (success bool, err error) {
	resp, err := c.Do("hdel", key, field)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) HIncr(key, field string, num int) (res int, err error) {
	resp, err := c.Do("hincr", key, field, num)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) HDecr(key, field string, num int) (res int, err error) {
	resp, err := c.Do("hdecr", key, field, num)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) HExists(key, field string) (exists bool, err error) {
	resp, err := c.Do("hexists", key, field)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		num, err2 := strconv.Atoi(resp[1])
		err = err2
		exists = num == 1
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) HSize(key string) (size int, err error) {
	resp, err := c.Do("hsize", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])

	}
	err = ErrBadResponse
	return
}

func (c *Client) HList(startKey, endKey string, limit int) (keyList []string,err error) {
	resp, err := c.Do("hlist", startKey, endKey, limit)
	if len(resp) > 0 && resp[0] == "ok" {
		keyList = resp[1:]
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) HKeys(key, startField, endField string, limit int) (fieldList []string, err error) {
	resp, err := c.Do("hkeys", key, startField, endField, limit)
	if len(resp) > 0 && resp[0] == "ok" {
		fieldList = resp[1:]
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) HScan(key, startField, endField string, limit int) (fvList [][2]string, err error) {
	resp, err := c.Do("hscan", key, startField, endField, limit)
	if len(resp)&1 == 1 && resp[0] == "ok" {
		fvList = [][2]string{}
		res := resp[1:]
		for i := 0; i < len(res); i += 2 {
			f := res[i]
			v := res[i+1]
			fv := [2]string{f, v}
			fvList = append(fvList, fv)
		}
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) HRScan(key, startField, endField string, limit int) (fvList [][2]string, err error) {
	resp, err := c.Do("hrscan", key, startField, endField, limit)
	if len(resp)&1 == 1 && resp[0] == "ok" {
		fvList = [][2]string{}
		res := resp[1:]
		for i := 0; i < len(res); i += 2 {
			f := res[i]
			v := res[i+1]
			fv := [2]string{f, v}
			fvList = append(fvList, fv)
		}
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) HClear(key string) (success bool, err error) {
	resp, err := c.Do("hclear", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) MultiHSet(key string, fvMap map[string]string) (success bool, err error) {
	if len(fvMap) == 0 {
		err = ErrNotEnoughParams
		return
	}
	args := []interface{}{"multi_hset", key}
	for f, v := range fvMap {
		args = append(args, f, v)
	}
	resp, err := c.Do(args...)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) MultiHGet(key string, fieldList []string) (fvMap map[string]string, err error) {
	if len(fieldList) == 0 {
		err = ErrNotEnoughParams
		return
	}
	args := []interface{}{"multi_hget", key}
	for _, f := range fieldList {
		args = append(args, f)
	}
	resp, err := c.Do(args...)
	if len(resp)&1 == 1 && resp[0] == "ok" {
		fvMap = map[string]string{}
		res := resp[1:]
		for i := 0; i < len(res); i += 2 {
			k := res[i]
			v := res[i+1]
			fvMap[k] = v
		}
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) MultiHDel(key string, fieldList []string) (success bool, err error) {
	if len(fieldList) == 0 {
		err = ErrNotEnoughParams
		return
	}
	args := []interface{}{"multi_del", key}
	for _, f := range fieldList {
		args = append(args, f)
	}
	resp, err := c.Do(args...)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

//Key-Zset
func (c *Client) ZSet(key, ele string, score int) (success bool, err error) {
	resp, err := c.Do("zset", key, ele, score)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZGet(key, ele string) (score interface{}, err error) {
	resp, err := c.Do("zget", key, ele)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZDel(key, ele string) (success bool, err error) {
	resp, err := c.Do("zdel", key, ele)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZIncr(key, ele string, num int) (score int, err error) {
	resp, err := c.Do("zincr", key, ele, num)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZSize(key string) (size int, err error) {
	resp, err := c.Do("zsize", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZExists(key, ele string) (exists bool, err error) {
	resp, err := c.Do("zexists", key, ele)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		num, err2 := strconv.Atoi(resp[1])
		if err2 != nil {
			err = err2
			return
		}
		exists = num > 0
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZList(startKey, endKey string, limit int) (keyList []string, err error) {
	resp, err := c.Do("zlist", startKey, endKey, limit)
	if err != nil {
		return
	}
	if len(resp) > 0 && resp[0] == "ok" {
		keyList = resp[1:]
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZKeys(key, startEle string, scoreStart, scoreEnd, limit int) (keyList []string, err error) {
	resp, err := c.Do("zkeys", key, startEle, scoreStart, scoreEnd, limit)
	if err != nil {
		return
	}
	if len(resp) > 0 && resp[0] == "ok" {
		keyList = resp[1:]
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZScan(key, startEle string, scoreStart, scoreEnd, limit int) (esMap map[string]int, err error) {
	resp, err := c.Do("zscan", key, startEle, scoreStart, scoreEnd, limit)
	if err != nil {
		return
	}
	if len(resp) > 0 && resp[0] == "ok" {
		res := resp[1:]
		esMap = map[string]int{}
		for i := 0; i < len(res); i += 2 {
			k := res[i]
			v, err2 := strconv.Atoi(res[i+1])
			if err2 != nil {
				err = err2
				return
			}
			esMap[k] = v
		}
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZRScan(key, startEle string, scoreStart, scoreEnd, limit int) (esMap map[string]int, err error) {
	resp, err := c.Do("zrscan", key, startEle, scoreStart, scoreEnd, limit)
	if err != nil {
		return
	}
	if len(resp) > 0 && resp[0] == "ok" {
		res := resp[1:]
		esMap = map[string]int{}
		for i := 0; i < len(res); i += 2 {
			e := res[i]
			s, err2 := strconv.Atoi(res[i+1])
			if err2 != nil {
				err = err2
				return
			}
			esMap[e] = s
		}
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZRank(key, ele string) (score int, err error) {
	resp, err := c.Do("zrank", key, ele)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZRRank(key, ele string) (score int, err error) {
	resp, err := c.Do("zrrank", key, ele)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZRange(key string, offset, limit int) (esList [][2]interface{}, err error) {
	resp, err := c.Do("zrange", key, offset, limit)
	if err != nil {
		return
	}
	if len(resp)&1 == 1 && resp[0] == "ok" {
		res := resp[1:]
		esList = [][2]interface{}{}
		for i := 0; i < len(res); i += 2 {
			e := res[i]
			s, err2 := strconv.Atoi(res[i+1])
			if err2 != nil {
				err = err2
				return
			}
			es := [2]interface{}{e, s}
			esList = append(esList, es)
		}
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZRRange(key string, offset, limit int) (esList [][2]interface{}, err error) {
	resp, err := c.Do("zrrange", key, offset, limit)
	if err != nil {
		return
	}
	if len(resp)&1 == 1 && resp[0] == "ok" {
		res := resp[1:]
		esList = [][2]interface{}{}
		for i := 0; i < len(res); i += 2 {
			e := res[i]
			s, err2 := strconv.Atoi(res[i+1])
			if err2 != nil {
				err = err2
				return
			}
			es := [2]interface{}{e, s}
			esList = append(esList, es)
		}
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) ZClear(key string) (success bool, err error) {
	resp, err := c.Do("zclear", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) MultiZSet(key string, esMap map[string]int) (success bool, err error) {
	if len(esMap) == 0 {
		err = ErrNotEnoughParams
		return
	}
	args := []interface{}{"multi_zset", key}
	for e, s := range esMap {
		args = append(args, e, s)
	}
	resp, err := c.Do(args...)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) MultiZGet(key string, eleList []string) (esMap map[string]int, err error) {
	if len(eleList) == 0 {
		err = ErrNotEnoughParams
		return
	}
	args := []interface{}{"multi_zget", key}
	for _, e := range eleList {
		args = append(args, e)
	}
	resp, err := c.Do(args...)
	if len(resp)&1 == 1 && resp[0] == "ok" {
		res := resp[1:]
		esMap = map[string]int{}
		for i := 0; i < len(res); i += 2 {
			k := res[i]
			v, err2 := strconv.Atoi(res[i+1])
			if err2 != nil {
				err = err2
				return
			}
			esMap[k] = v
		}
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) MultiZDel(key string, eleList []string) (success bool, err error) {
	if len(eleList) == 0 {
		err = ErrNotEnoughParams
		return
	}
	args := []interface{}{"multi_zdel", key}
	for _, e := range eleList {
		args = append(args, e)
	}
	resp, err := c.Do(args...)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

//Key-List/Queue
func (c *Client) QSzie(key string) (size int, err error) {
	resp, err := c.Do("qsize", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		return strconv.Atoi(resp[1])
	}
	err = ErrBadResponse
	return
}

func (c *Client) QClear(key string) (success bool, err error) {
	resp, err := c.Do("qclear", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) QFront(key string) (item string, err error) {
	resp, err := c.Do("qfront", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		item = resp[1]
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) QBack(key string) (item string, err error) {
	resp, err := c.Do("qback", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		item = resp[1]
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) QGet(key string, index int) (item interface{}, err error) {
	resp, err := c.Do("qget", key, index)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		item = resp[1]
		return
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) QSlice(key string, begin, end int) (itemList []string, err error) {
	resp, err := c.Do("qslice", key, begin, end)
	if err != nil {
		return
	}
	if len(resp) > 0 && resp[0] == "ok" {
		itemList = resp[1:]
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) QPush(key, item string) (success bool, err error) {
	return c.QPushBack(key, item)
}

func (c *Client) QPushFront(key, item string) (success bool, err error) {
	resp, err := c.Do("qpush_front", key, item)
	if err != nil {
		return
	}
	if len(resp) == 1 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
	
}

func (c *Client) QPushBack(key, item string) (success bool, err error) {
	resp, err := c.Do("qpush_back", key, item)
	if err != nil {
		return
	}
	if len(resp) == 1 && resp[0] == "ok" {
		success = true
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) QPop(key string) (ele interface{}, err error) {
	return c.QPopFront(key)
}

func (c *Client) QPopFront(key string) (ele interface{}, err error) {
	resp, err := c.Do("qpop_front", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		ele = resp[1]
		return
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return
	}
	err = ErrBadResponse
	return
}

func (c *Client) QPopBack(key string) (ele interface{}, err error) {
	resp, err := c.Do("qpop_back", key)
	if err != nil {
		return
	}
	if len(resp) == 2 && resp[0] == "ok" {
		ele = resp[1]
		return
	}
	if len(resp) > 0 && resp[0] == "not_found" {
		return
	}
	err = ErrBadResponse
	return
}

//base
func (c *Client) send(args []interface{}) error {
	var buf bytes.Buffer
	for _, arg := range args {
		var s string
		switch arg := arg.(type) {
		case string:
			s = arg
		case []byte:
			s = string(arg)
		case int:
			s = fmt.Sprintf("%d", arg)
		case int64:
			s = fmt.Sprintf("%d", arg)
		case float64:
			s = fmt.Sprintf("%f", arg)
		case bool:
			if arg {
				s = "1"
			} else {
				s = "0"
			}
		case nil:
			s = ""
		default:
			return fmt.Errorf("bad arguments")
		}
		buf.WriteString(fmt.Sprintf("%d", len(s)))
		buf.WriteByte('\n')
		buf.WriteString(s)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	_, err := c.sock.Write(buf.Bytes())
	return err
}

func (c *Client) recv() ([]string, error) {
	var tmp [8192]byte
	for {
		n, err := c.sock.Read(tmp[0:])
		if err != nil {
			return nil, err
		}
		c.recv_buf.Write(tmp[0:n])
		resp := c.parse()
		if resp == nil || len(resp) > 0 {
			return resp, nil
		}
	}
}

func (c *Client) parse() []string {
	resp := []string{}
	buf := c.recv_buf.Bytes()
	var idx, offset int
	idx = 0
	offset = 0

	for {
		idx = bytes.IndexByte(buf[offset:], '\n')
		if idx == -1 {
			break
		}
		p := buf[offset : offset+idx]
		offset += idx + 1
		//fmt.Printf("> [%s]\n", p);
		if len(p) == 0 || (len(p) == 1 && p[0] == '\r') {
			if len(resp) == 0 {
				continue
			} else {
				c.recv_buf.Next(offset)
				return resp
			}
		}

		size, err := strconv.Atoi(string(p))
		if err != nil || size < 0 {
			return nil
		}
		if offset+size >= c.recv_buf.Len() {
			break
		}

		v := buf[offset : offset+size]
		resp = append(resp, string(v))
		offset += size + 1
	}

	return []string{}
}

// Close The Client Connection
func (c *Client) Close() error {
	return c.sock.Close()
}
