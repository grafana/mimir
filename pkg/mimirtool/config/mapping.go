// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/grafana/dskit/multierror"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/storage/bucket/s3"
)

type Mapper interface {
	DoMap(source, target *InspectedEntry) error
}

type Mapping func(oldPath string, oldVal interface{}) (newPath string, newVal interface{})

// BestEffortDirectMapper implement Mapper and naively maps the values of all parameters form the source to the
// same name parameter in the target. It ignores all errors while setting the values.
type BestEffortDirectMapper struct{}

func (BestEffortDirectMapper) DoMap(source, target *InspectedEntry) error {
	err := source.Walk(func(path string, value interface{}) error {
		_ = target.SetValue(path, value)
		return nil
	})
	if err != nil {
		panic("walk returned an error, even though the walking function didn't return any, not sure what to do: " + err.Error())
	}
	return nil
}

// PathMapper applies the mappings to specific parameters of the source.
type PathMapper struct {
	PathMappings map[string]Mapping
}

// DoMap applies the Mappings from source to target.
// The error DoMap returns are the combined errors that all Mappings returned. If no
// Mappings returned an error, then DoMap returns nil.
func (m PathMapper) DoMap(source, target *InspectedEntry) error {
	errs := multierror.New()
	for path, mapping := range m.PathMappings {
		oldVal, err := source.GetValue(path)
		if err != nil {
			errs.Add(err)
			continue
		}
		err = target.SetValue(mapping(path, oldVal))
		if err != nil {
			errs.Add(err)
		}
	}
	return errs.Err()
}

type NoopMapper struct{}

func (NoopMapper) DoMap(_, _ *InspectedEntry) error {
	return nil
}

type MultiMapper []Mapper

func (m MultiMapper) DoMap(source, target *InspectedEntry) error {
	errs := multierror.New()
	for _, mapper := range m {
		errs.Add(mapper.DoMap(source, target))
	}
	return errs.Err()
}

type MapperFunc func(source, target *InspectedEntry) error

func (m MapperFunc) DoMap(source, target *InspectedEntry) error {
	return m(source, target)
}

func RenameMapping(to string) Mapping {
	return func(oldPath string, oldVal interface{}) (newPath string, newVal interface{}) {
		newPath = to
		newVal = oldVal
		return
	}
}

func alertmanagerURLMapperFunc(source, target *InspectedEntry) error {
	amDiscovery, err := source.GetValue("ruler.enable_alertmanager_discovery")
	if err != nil {
		return errors.Wrap(err, "could not convert ruler.enable_alertmanager_discovery")
	}
	if amDiscovery == nil || !amDiscovery.(bool) {
		return nil
	}

	amURL, err := source.GetValue("ruler.alertmanager_url")
	if err != nil {
		return errors.Wrap(err, "could not get ruler.alertmanager_url")
	}

	amURLs := strings.Split(amURL.(string), ",")
	for i := range amURLs {
		amURLs[i] = "dnssrvnoa+" + amURLs[i]
	}
	return target.SetValue("ruler.alertmanager_url", strings.Join(amURLs, ","))
}

// rulerStorageMapperFunc returns a MapperFunc that maps alertmanager.storage and alertmanager_storage to alertmanager_storage.
// Values from alertmanager.storage take precedence.
func alertmanagerStorageMapperFunc(sourceDefaults *InspectedEntry) MapperFunc {
	return func(source, target *InspectedEntry) error {
		_, err := source.GetValue("alertmanager.storage.type")
		if err != nil {
			// When doing flag mappings this function gets called with a source config that
			// contains only the values we have from flags. In order for the code later to not
			// panic, we do a quick check if the source contains a parameter we expect to exist.
			//
			// Known bug: This also means that if a user has passed only their -alertmanager.storage.type
			// as a flag, this check will pass and the below will still panic. This should be uncommon.
			return err
		}

		pathRenames := map[string]string{
			"alertmanager.storage.azure.account_key":                      "alertmanager_storage.azure.account_key",
			"alertmanager.storage.azure.account_name":                     "alertmanager_storage.azure.account_name",
			"alertmanager.storage.azure.container_name":                   "alertmanager_storage.azure.container_name",
			"alertmanager.storage.azure.max_retries":                      "alertmanager_storage.azure.max_retries",
			"alertmanager.storage.gcs.bucket_name":                        "alertmanager_storage.gcs.bucket_name",
			"alertmanager.storage.local.path":                             "alertmanager_storage.local.path",
			"alertmanager.storage.s3.access_key_id":                       "alertmanager_storage.s3.access_key_id",
			"alertmanager.storage.s3.bucketnames":                         "alertmanager_storage.s3.bucket_name", // if it is comma-delimited, then it's invalid
			"alertmanager.storage.s3.endpoint":                            "alertmanager_storage.s3.endpoint",    // if it is already set by the previous mapping, then err
			"alertmanager.storage.s3.http_config.idle_conn_timeout":       "alertmanager_storage.s3.http.idle_conn_timeout",
			"alertmanager.storage.s3.http_config.insecure_skip_verify":    "alertmanager_storage.s3.http.insecure_skip_verify",
			"alertmanager.storage.s3.http_config.response_header_timeout": "alertmanager_storage.s3.http.response_header_timeout",
			"alertmanager.storage.s3.insecure":                            "alertmanager_storage.s3.insecure",
			"alertmanager.storage.s3.region":                              "alertmanager_storage.s3.region",
			//"alertmanager.storage.s3.s3":                                  RenameMapping("alertmanager_storage.s3.endpoint"), // if it contains "inmemory://" this should be invalid, also how do we know if the URL contains "escaped Key and Secret encoded"?
			"alertmanager.storage.s3.secret_access_key":          "alertmanager_storage.s3.secret_access_key",
			"alertmanager.storage.s3.signature_version":          "alertmanager_storage.s3.signature_version",
			"alertmanager.storage.s3.sse.kms_encryption_context": "alertmanager_storage.s3.sse.kms_encryption_context",
			"alertmanager.storage.s3.sse.kms_key_id":             "alertmanager_storage.s3.sse.kms_key_id",
			"alertmanager.storage.s3.sse.type":                   "alertmanager_storage.s3.sse.type",
			"alertmanager.storage.type":                          "alertmanager_storage.backend",
		}

		return mapDotStorage(pathRenames, source, target, sourceDefaults)
	}
}

// rulerStorageMapperFunc returns a MapperFunc that maps ruler.storage and ruler_storage to ruler_storage.
// Values from ruler.storage take precedence.
func rulerStorageMapperFunc(sourceDefaults *InspectedEntry) MapperFunc {
	return func(source, target *InspectedEntry) error {
		_, err := source.GetValue("ruler.storage.type")
		if err != nil {
			// When doing flag mappings this function gets called with a source config that
			// contains only the values we have from flags. In order for the code later to not
			// panic, we do a quick check if the source contains a parameter we expect to exist.
			//
			// Known bug: This also means that if a user has passed only their -ruler.storage.type
			// as a flag, this check will pass and the below will still panic. This should be uncommon.
			return err
		}

		pathRenames := map[string]string{
			"ruler.storage.azure.account_key":                      "ruler_storage.azure.account_key",
			"ruler.storage.azure.account_name":                     "ruler_storage.azure.account_name",
			"ruler.storage.azure.container_name":                   "ruler_storage.azure.container_name",
			"ruler.storage.azure.max_retries":                      "ruler_storage.azure.max_retries",
			"ruler.storage.gcs.bucket_name":                        "ruler_storage.gcs.bucket_name",
			"ruler.storage.local.directory":                        "ruler_storage.local.directory",
			"ruler.storage.s3.access_key_id":                       "ruler_storage.s3.access_key_id",
			"ruler.storage.s3.bucketnames":                         "ruler_storage.s3.bucket_name", // if it is comma-delimited, then it's invalid
			"ruler.storage.s3.endpoint":                            "ruler_storage.s3.endpoint",    // if it is already set by the previous mapping, then err
			"ruler.storage.s3.http_config.idle_conn_timeout":       "ruler_storage.s3.http.idle_conn_timeout",
			"ruler.storage.s3.http_config.insecure_skip_verify":    "ruler_storage.s3.http.insecure_skip_verify",
			"ruler.storage.s3.http_config.response_header_timeout": "ruler_storage.s3.http.response_header_timeout",
			"ruler.storage.s3.insecure":                            "ruler_storage.s3.insecure",
			"ruler.storage.s3.region":                              "ruler_storage.s3.region",
			//"ruler.storage.s3.s3":                                  RenameMapping("ruler_storage.s3.endpoint"), // if it contains "inmemory://" this should be invalid, also how do we know if the URL contains "escaped Key and Secret encoded"?
			"ruler.storage.s3.secret_access_key":          "ruler_storage.s3.secret_access_key",
			"ruler.storage.s3.signature_version":          "ruler_storage.s3.signature_version",
			"ruler.storage.s3.sse.kms_encryption_context": "ruler_storage.s3.sse.kms_encryption_context",
			"ruler.storage.s3.sse.kms_key_id":             "ruler_storage.s3.sse.kms_key_id",
			"ruler.storage.s3.sse.type":                   "ruler_storage.s3.sse.type",
			"ruler.storage.swift.auth_url":                "ruler_storage.swift.auth_url",
			"ruler.storage.swift.auth_version":            "ruler_storage.swift.auth_version",
			"ruler.storage.swift.connect_timeout":         "ruler_storage.swift.connect_timeout",
			"ruler.storage.swift.container_name":          "ruler_storage.swift.container_name",
			"ruler.storage.swift.domain_id":               "ruler_storage.swift.domain_id",
			"ruler.storage.swift.domain_name":             "ruler_storage.swift.domain_name",
			"ruler.storage.swift.max_retries":             "ruler_storage.swift.max_retries",
			"ruler.storage.swift.password":                "ruler_storage.swift.password",
			"ruler.storage.swift.project_domain_id":       "ruler_storage.swift.project_domain_id",
			"ruler.storage.swift.project_domain_name":     "ruler_storage.swift.project_domain_name",
			"ruler.storage.swift.project_id":              "ruler_storage.swift.project_id",
			"ruler.storage.swift.project_name":            "ruler_storage.swift.project_name",
			"ruler.storage.swift.region_name":             "ruler_storage.swift.region_name",
			"ruler.storage.swift.request_timeout":         "ruler_storage.swift.request_timeout",
			"ruler.storage.swift.user_domain_id":          "ruler_storage.swift.user_domain_id",
			"ruler.storage.swift.user_domain_name":        "ruler_storage.swift.user_domain_name",
			"ruler.storage.swift.user_id":                 "ruler_storage.swift.user_id",
			"ruler.storage.swift.username":                "ruler_storage.swift.username",
		}

		return mapDotStorage(pathRenames, source, target, sourceDefaults)
	}
}

func mapDotStorage(pathRenames map[string]string, source, target, sourceDefaults *InspectedEntry) error {
	mapper := &PathMapper{PathMappings: map[string]Mapping{}}
	for dotStoragePath, storagePath := range pathRenames {
		// if the ruler.storage was set, then use that in the final config
		if !reflect.DeepEqual(source.MustGetValue(dotStoragePath), sourceDefaults.MustGetValue(dotStoragePath)) {
			mapper.PathMappings[dotStoragePath] = RenameMapping(storagePath)
			continue
		}

		// if the ruler_storage was set to something other than the default, then we
		// take that value as the one in the final config.
		if !reflect.DeepEqual(source.MustGetValue(storagePath), sourceDefaults.MustGetValue(storagePath)) {
			mapper.PathMappings[storagePath] = RenameMapping(storagePath)
		}
	}

	return mapper.DoMap(source, target)
}

// mapS3SSE maps (alertmanager|ruler).storage.s3.sse_encryption to (alertmanager|ruler)_storage.s3.sse.type.
// prefix should be either "alertmanager" or "ruler". If <prefix>.storage.s3.sse_encryption was true,
// it is replaced by alertmanager_storage.s3.sse.type="SSE-S3"
func mapS3SSE(prefix string) MapperFunc {
	return func(source, target *InspectedEntry) error {
		var (
			sseEncryptionPath = prefix + ".storage.s3.sse_encryption"
			sseTypePath       = prefix + "_storage.s3.sse.type"
		)

		sseWasEnabledVal, err := source.GetValue(sseEncryptionPath)
		if err != nil {
			return err
		}
		if sseWasEnabledVal.(bool) && target.MustGetValue(sseTypePath) == "" {
			return target.SetValue(sseTypePath, s3.SSES3)
		}

		return nil
	}
}

// mapMemcachedAddresses maps query_range...memcached_client.host and .service to a DNS Service Discovery format
// address. This should preserve the behaviour in cortex v1.11.0:
// https://github.com/cortexproject/cortex/blob/43c646ba3ff906e80a6a1812f2322a0c276e9deb/pkg/chunk/cache/memcached_client.go#L242-L258
func mapMemcachedAddresses(source, target *InspectedEntry) error {
	const (
		oldPrefix = "query_range.results_cache.cache.memcached_client"
		newPrefix = "frontend.results_cache.memcached"
	)
	presetAddressesVal, err := source.GetValue(oldPrefix + ".addresses")
	if err != nil {
		return err
	}
	if presetAddressesVal.(string) != "" {
		return nil // respect already set values of addresses
	}

	service, hostname := source.MustGetValue(oldPrefix+".service"), source.MustGetValue(oldPrefix+".host")
	newAddress := fmt.Sprintf("dnssrvnoa+_%s._tcp.%s", service, hostname)

	return target.SetValue(newPrefix+".addresses", newAddress)
}
