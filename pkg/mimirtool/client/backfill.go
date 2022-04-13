package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/tsdb"
)

func (c *MimirClient) Backfill(ctx context.Context, source string, tenantID int, logger log.Logger) error {
	// Scan blocks in source directory
	es, err := os.ReadDir(source)
	if err != nil {
		return errors.Wrapf(err, "failed to read directory %q", source)
	}
	for _, e := range es {
		if err := c.backfillBlock(filepath.Join(source, e.Name()), tenantID, logger); err != nil {
			return err
		}
	}

	return nil
}

func (c *MimirClient) backfillBlock(dpath string, tenantID int, logger log.Logger) error {
	metaPath := filepath.Join(dpath, "meta.json")
	f, err := os.Open(metaPath)
	if err != nil {
		return errors.Wrapf(err, "failed to open %q", metaPath)
	}

	dec := json.NewDecoder(f)
	var blockMeta tsdb.BlockMeta
	if err := dec.Decode(&blockMeta); err != nil {
		return errors.Wrapf(err, "failed to decode %q", metaPath)
	}

	blockID := filepath.Base(dpath)

	level.Info(logger).Log("msg", "Making request to start block backfill", "tenantId", tenantID, "blockId", blockID)

	// TODO: Figure out how to set tenant ID in request header
	res, err := c.doRequest(fmt.Sprintf("/api/v1/backfill/%d/%s", tenantID, blockID), http.MethodPost, nil, -1)
	if err != nil {
		return errors.Wrap(err, "request to start backfill failed")
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("request to start backfill failed, status code %d", res.StatusCode)
	}

	if err := filepath.WalkDir(dpath, func(pth string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}

		f, err := os.Open(pth)
		if err != nil {
			return errors.Wrapf(err, "failed to open %q", pth)
		}
		defer f.Close()

		st, err := f.Stat()
		if err != nil {
			return errors.Wrap(err, "failed to get file info")
		}

		relPath := strings.TrimPrefix(pth, dpath+string(filepath.Separator))
		escapedPath := url.PathEscape(relPath)
		level.Info(logger).Log("msg", "uploading block file", "path", pth, "tenantId",
			tenantID, "blockId", blockID, "size", st.Size())
		res, err := c.doRequest(fmt.Sprintf("/api/v1/backfill/%d/%s/%s", tenantID, blockID,
			escapedPath), http.MethodPost, f, st.Size())
		if err != nil {
			return errors.Wrapf(err, "request to upload backfill of file %q failed", pth)
		}
		defer res.Body.Close()
		if res.StatusCode/100 != 2 {
			return fmt.Errorf("request to upload backfill file failed, status code %d", res.StatusCode)
		}

		return nil
	}); err != nil {
		return errors.Wrapf(err, "failed to traverse %q", dpath)
	}

	res, err = c.doRequest(fmt.Sprintf("/api/v1/backfill/%d/%s", tenantID, blockID), http.MethodDelete,
		nil, -1)
	if err != nil {
		return errors.Wrap(err, "request to finish backfill failed")
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("request to finish backfill failed, status code %d", res.StatusCode)
	}

	level.Info(logger).Log("msg", "Block backfill successful", "tenantId", tenantID, "blockId", blockID)

	return nil
}
