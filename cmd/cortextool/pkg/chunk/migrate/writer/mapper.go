package writer

import (
	"os"

	"github.com/cortexproject/cortex/pkg/chunk"
	"gopkg.in/alecthomas/kingpin.v2"
	yaml "gopkg.in/yaml.v2"
)

// Map Config File
// The map config file is a yaml file structed as:
/*
users:
  user_original: user_mapped
  ...
  <user_id_src>: <user_id_dst>

*/

// MapperConfig is used to configure a migrate mapper
type MapperConfig struct {
	MapConfigFile string
}

// RegisterFlags adds the flags required to configure this flag set.
func (cfg *MapperConfig) Register(cmd *kingpin.CmdClause) {
	cmd.Flag("mapper.config", "file name for reader mapping configs").StringVar(&cfg.MapConfigFile)
}

// Mapper is used to update and reencode chunks with new User Ids
// It can also serve as a struct to map other aspects of chunks in
// the future as more migration needs arise
// TODO: Add functionality to add/edit/drop labels
type Mapper struct {
	Users map[string]string `yaml:"users,omitempty"`
}

// NewMapper returns a Mapper
func NewMapper(cfg MapperConfig) (*Mapper, error) {
	if cfg.MapConfigFile == "" {
		return &Mapper{map[string]string{}}, nil
	}
	return loadMapperConfig(cfg.MapConfigFile)
}

// MapChunks updates and maps values onto an array of chunks
func (u *Mapper) MapChunk(chk chunk.Chunk) (chunk.Chunk, error) {
	newID, ok := u.Users[chk.UserID]
	if ok {
		chk = chunk.NewChunk(newID, chk.Fingerprint, chk.Metric, chk.Data, chk.From, chk.Through)
		err := chk.Encode()
		if err != nil {
			return chk, err
		}
	}

	return chk, nil
}

func loadMapperConfig(filename string) (*Mapper, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	decoder := yaml.NewDecoder(f)
	decoder.SetStrict(true)

	chunkMapper := &Mapper{}

	if err := decoder.Decode(chunkMapper); err != nil {
		return nil, err
	}

	return chunkMapper, nil
}
