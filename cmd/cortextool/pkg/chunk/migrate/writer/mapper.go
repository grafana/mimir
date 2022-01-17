package writer

import (
	"github.com/grafana/mimir/pkg/chunk"
)

// Map Config File
// The map config file is a yaml file structed as:
/*
users:
  user_original: user_mapped
  ...
  <user_id_src>: <user_id_dst>

*/

// Mapper is used to update and reencode chunks with new User Ids
// It can also serve as a struct to map other aspects of chunks in
// the future as more migration needs arise
// TODO: Add functionality to add/edit/drop labels
type Mapper struct {
	Users map[string]string `yaml:"users,omitempty"`
}

// MapChunk updates and maps values onto a chunkl
func (u Mapper) MapChunk(chk chunk.Chunk) (chunk.Chunk, error) {
	if u.Users != nil {
		newID, ok := u.Users[chk.UserID]
		if ok {
			chk = chunk.NewChunk(newID, chk.Fingerprint, chk.Metric, chk.Data, chk.From, chk.Through)
			err := chk.Encode()
			if err != nil {
				return chk, err
			}
		}
	}
	return chk, nil
}
