package store

type PersistItem struct {
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	Flags     int    `json:"flags"`
	Exptime   int64  `json:"exptime"`
	CasUnique int64  `json:"casUnique"`
}
