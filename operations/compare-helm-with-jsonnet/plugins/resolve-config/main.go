// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/pkg/errors"
	"sigs.k8s.io/kustomize/kyaml/fn/framework"
	"sigs.k8s.io/kustomize/kyaml/fn/framework/command"
	"sigs.k8s.io/kustomize/kyaml/kio"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/grafana/mimir/pkg/mimir"
)

const MimirImage = "grafana/mimir"

type ValueAnnotator struct {
	Value string `yaml:"value" json:"value"`
}

func main() {
	config := new(ValueAnnotator)
	fn := func(items []*yaml.RNode) ([]*yaml.RNode, error) {
		mapper := NewConfigExtractor(items)
		configs, err := mapper.ResolveConfigs()
		if err != nil {
			return nil, err
		}
		return append(items, configs...), nil
	}
	p := framework.SimpleProcessor{Config: config, Filter: kio.FilterFunc(fn)}
	cmd := command.Build(p, command.StandaloneDisabled, false)
	command.AddGenerateDockerfile(cmd)
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

type ConfigExtractor struct {
	allItems []*yaml.RNode
}

func NewConfigExtractor(items []*yaml.RNode) *ConfigExtractor {
	return &ConfigExtractor{
		allItems: items,
	}
}

// PodSpec is a subset from k8s.io/api/core/v1.PodSpec
// copied here in order to avoid a dependency on the k8s.io/api/core/v1 package
type PodSpec struct {
	Volumes []struct {
		Name      string `yaml:"name"`
		ConfigMap *struct {
			Name string `yaml:"name"`
		} `yaml:"configMap"`
		Secret *struct {
			SecretName string `yaml:"secretName"`
		} `yaml:"secret"`
		EmptyDir *struct{}
	} `yaml:"volumes"`
	Containers []struct {
		Args         []string `yaml:"args"`
		Image        string   `yaml:"image"`
		VolumeMounts []struct {
			Name      string `yaml:"name"`
			MountPath string `yaml:"mountPath"`
		} `yaml:"volumeMounts"`
	} `yaml:"containers"`
}

// DeploymentOrStatefulSet is a subset from k8s.io/api/apps/v1.Deployment and k8s.io/api/apps/v1.StatefulSet
// copied here in order to avoid a dependency on the k8s.io/api/apps/v1 package
type DeploymentOrStatefulSet struct {
	Spec struct {
		Template struct {
			Spec PodSpec `yaml:"spec"`
		} `yaml:"template"`
	} `yaml:"spec"`
}

func extractPodSpec(obj *yaml.RNode) (PodSpec, bool, error) {
	switch obj.GetKind() {
	case "Deployment", "StatefulSet":
		var dep = new(DeploymentOrStatefulSet)
		err := decodeTypedObject(obj, &dep)
		if err != nil {
			return PodSpec{}, false, err
		}
		return dep.Spec.Template.Spec, true, nil
	}

	return PodSpec{}, false, nil
}

func (c *ConfigExtractor) ResolveConfigs() ([]*yaml.RNode, error) {
	results := make(chan *yaml.RNode, len(c.allItems))

	defaultObj, _, err := c.resolveArgsAndConfigFile(nil, "")
	if err != nil {
		return nil, err
	}

	err = concurrency.ForEachJob(context.Background(), len(c.allItems), runtime.NumCPU(), func(_ context.Context, idx int) error {
		pod, ok, err := extractPodSpec(c.allItems[idx])
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		for _, container := range pod.Containers {
			if !strings.HasPrefix(container.Image, MimirImage) {
				continue
			}

			var configFileText string
			mountPath, args := findConfigFileArg(container.Args)
			if mountPath != "" {
				for _, vm := range container.VolumeMounts {
					if !strings.HasPrefix(mountPath, vm.MountPath) {
						continue
					}

					content, err := c.resolveConfigFileText(vm.Name, filepath.Base(mountPath), pod)
					if err != nil {
						return errors.Wrapf(err, "failed to resolve volume mount %s in pod", vm.Name)
					}

					configFileText = content
				}
			}

			configObj, target, err := c.resolveArgsAndConfigFile(args, configFileText)
			if err != nil {
				return errors.Wrap(err, "failed to resolve config")
			}

			err = annotateDefaults(configObj, defaultObj)
			if err != nil {
				return errors.Wrap(err, "failed to annotate defaults")
			}

			resultNode := yaml.MustParse(`
apiVersion: grafana.com/v1alpha1
kind: MimirConfig
metadata:
  name: mimir-config
  namespace: default
`)
			err = resultNode.SetName(target)
			if err != nil {
				return errors.Wrap(err, "failed to set name")
			}
			err = resultNode.SetMapField(configObj, "config")
			if err != nil {
				return errors.Wrap(err, "failed to set config")
			}

			results <- resultNode
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	close(results)

	rawConfigs := []*yaml.RNode{}
	for r := range results {
		rawConfigs = append(rawConfigs, r)
	}

	return rawConfigs, nil
}

func (c *ConfigExtractor) resolveArgsAndConfigFile(args []string, configFileText string) (*yaml.RNode, string, error) {
	var mimirConfig = &mimir.Config{}

	flagSet := flag.NewFlagSet("mimir", flag.ContinueOnError)
	// Suppress usage output
	flagSet.Usage = func() {}

	// This sets default values on the config struct, so it needs to be called before parsing the config file
	mimirConfig.RegisterFlags(flagSet, log.NewNopLogger())

	// These values are required because they might be set by some templates but they don't exist on the configuration struct
	// There are other flags defined in main.go that could one day be used, and this would cause the flagSet.Parse() to fail
	flagSet.Bool("config.expand-env", false, "")
	flagSet.Int("mem-ballast-size-bytes", 0, "")

	err := yaml.Unmarshal([]byte(configFileText), mimirConfig)
	if err != nil {
		return nil, "", err
	}

	err = flagSet.Parse(args)
	if err != nil {
		return nil, "", err
	}

	finalConfigBytes, err := yaml.Marshal(mimirConfig)
	if err != nil {
		return nil, "", err
	}

	configRNode, err := yaml.Parse(string(finalConfigBytes))
	if err != nil {
		return nil, "", err
	}

	return configRNode, strings.Join(mimirConfig.Target, ","), nil
}

func findConfigFileArg(args []string) (string, []string) {
	for i, arg := range args {
		if strings.Contains(arg, "config.file") {
			return strings.TrimPrefix(arg, "-config.file="), append(args[:i], args[i+1:]...)
		}
	}
	return "", args
}

func (c *ConfigExtractor) resolveConfigFileText(volumeName, fileName string, spec PodSpec) (string, error) {
	for _, volume := range spec.Volumes {
		if volume.Name == volumeName {
			if volume.ConfigMap != nil {
				text, err := c.resolveConfigMap(volume.ConfigMap.Name, fileName)
				if err != nil {
					return "", errors.Wrapf(err, "failed to resolve config map %s", volume.ConfigMap.Name)
				}
				return text, nil
			}
			if volume.Secret != nil {
				text, err := c.resolveSecret(volume.Secret.SecretName, fileName)
				if err != nil {
					return "", errors.Wrapf(err, "failed to resolve secret %s", volume.Secret.SecretName)
				}
				return text, nil
			}
			if volume.EmptyDir != nil {
				return "", nil
			}

			return "", errors.Errorf("unsupported volume type: %v", volume)
		}
	}

	return "", nil
}

func (c *ConfigExtractor) resolveConfigMap(name string, fileName string) (string, error) {
	for _, obj := range c.allItems {
		if obj.GetKind() != "ConfigMap" {
			continue
		}
		if obj.GetName() != name {
			continue
		}
		return obj.GetDataMap()[fileName], nil
	}

	return "", errors.Errorf("config map %s not found", name)
}

func (c *ConfigExtractor) resolveSecret(name string, fileName string) (string, error) {

	for _, obj := range c.allItems {
		if obj.GetKind() != "Secret" {
			continue
		}
		if obj.GetName() != name {
			continue
		}
		b64Data := obj.GetDataMap()[fileName]
		data, err := base64.StdEncoding.DecodeString(b64Data)
		return string(data), err
	}

	return "", errors.Errorf("secret %s not found", name)
}

func decodeTypedObject(obj *yaml.RNode, output interface{}) error {
	buf := new(bytes.Buffer)
	err := yaml.NewEncoder(buf).Encode(obj.YNode())
	if err != nil {
		return err
	}

	err = yaml.NewDecoder(buf).Decode(output)
	return err
}

func annotateDefaults(configObj *yaml.RNode, defaultObj *yaml.RNode) error {
	if configObj.IsNil() {
		return nil
	}

	switch configObj.YNode().Kind {
	case yaml.DocumentNode:
		return errors.Errorf("unsupported config object type: %v", configObj.YNode().Kind)
	case yaml.MappingNode:
		return configObj.VisitFields(func(node *yaml.MapNode) error {
			defaultField := defaultObj.Field(node.Key.YNode().Value)
			if defaultField == nil {
				return nil
			}
			return annotateDefaults(node.Value, defaultField.Value)
		})
	case yaml.SequenceNode:
		configElements, err := configObj.Elements()
		if err != nil {
			return err
		}
		defaultElements, err := defaultObj.Elements()
		if err != nil {
			return err
		}
		if len(configElements) != len(defaultElements) {
			return nil
		}
		for i := range configElements {
			err := annotateDefaults(configElements[i], defaultElements[i])
			if err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		if reflect.DeepEqual(configObj.YNode().Value, defaultObj.YNode().Value) {
			configObj.YNode().SetString(fmt.Sprintf("%s (default)", configObj.YNode().Value))
		}
	default:
		return errors.Errorf("unsupported config type: %v", configObj.YNode().Kind)
	}
	return nil
}
