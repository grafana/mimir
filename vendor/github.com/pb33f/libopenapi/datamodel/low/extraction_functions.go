// Copyright 2022 Princess B33f Heavy Industries / Dave Shanley
// SPDX-License-Identifier: MIT

package low

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"net/url"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	jsonpathconfig "github.com/pb33f/jsonpath/pkg/jsonpath/config"

	"github.com/pb33f/jsonpath/pkg/jsonpath"
	"github.com/pb33f/libopenapi/datamodel"
	"github.com/pb33f/libopenapi/index"
	"github.com/pb33f/libopenapi/orderedmap"
	"github.com/pb33f/libopenapi/utils"
	"go.yaml.in/yaml/v4"
)

// stringBuilderPool is a sync.Pool that reuses strings.Builder instances to reduce memory allocations
// when generating hashes across the codebase.
var stringBuilderPool = sync.Pool{
	New: func() interface{} {
		return new(strings.Builder)
	},
}

// hashCache is a global cache for computed hash values to avoid redundant calculations.
// Uses sync.Map for thread-safe concurrent access.
var hashCache sync.Map

// ClearHashCache clears the global hash cache. This should be called before
// starting a new document comparison to ensure clean state.
func ClearHashCache() {
	hashCache = sync.Map{}
}

// GetStringBuilder retrieves a strings.Builder from the pool, resets it, and returns it.
// The caller must call PutStringBuilder when done to return it to the pool.
func GetStringBuilder() *strings.Builder {
	sb := stringBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	return sb
}

// PutStringBuilder returns a strings.Builder to the pool for reuse.
func PutStringBuilder(sb *strings.Builder) {
	stringBuilderPool.Put(sb)
}

// FindItemInOrderedMap accepts a string key and a collection of KeyReference[string] and ValueReference[T].
// Every KeyReference will have its value checked against the string key and if there is a match, it will be
// returned.
func FindItemInOrderedMap[T any](item string, collection *orderedmap.Map[KeyReference[string], ValueReference[T]]) *ValueReference[T] {
	_, v := FindItemInOrderedMapWithKey(item, collection)
	return v
}

// FindItemInOrderedMapWithKey is the same as FindItemInOrderedMap, except this code returns the key as well as the value.
func FindItemInOrderedMapWithKey[T any](item string, collection *orderedmap.Map[KeyReference[string], ValueReference[T]]) (*KeyReference[string], *ValueReference[T]) {
	for pair := orderedmap.First(collection); pair != nil; pair = pair.Next() {
		n := pair.Key()
		if n.Value == item {
			return &n, pair.ValuePtr()
		}
		if strings.EqualFold(item, n.Value) {
			return &n, pair.ValuePtr()
		}
	}
	return nil, nil
}

// HashExtensions will generate a hash from the low representation of extensions.
func HashExtensions(ext *orderedmap.Map[KeyReference[string], ValueReference[*yaml.Node]]) []string {
	f := []string{}

	for e, node := range orderedmap.SortAlpha(ext).FromOldest() {
		b, _ := yaml.Marshal(node.GetValue())
		f = append(f, fmt.Sprintf("%s-%x", e.Value, sha256.Sum256([]byte(b))))
	}

	return f
}

// helper function to generate a list of all the things an index should be searched for.
func generateIndexCollection(idx *index.SpecIndex) []func() map[string]*index.Reference {
	return []func() map[string]*index.Reference{
		idx.GetAllComponentSchemas,
		idx.GetMappedReferences,
		idx.GetAllExternalDocuments,
		idx.GetAllParameters,
		idx.GetAllHeaders,
		idx.GetAllCallbacks,
		idx.GetAllLinks,
		idx.GetAllExamples,
		idx.GetAllRequestBodies,
		idx.GetAllResponses,
		idx.GetAllSecuritySchemes,
	}
}

func LocateRefNodeWithContext(ctx context.Context, root *yaml.Node, idx *index.SpecIndex) (*yaml.Node, *index.SpecIndex, error, context.Context) {
	if rf, _, rv := utils.IsNodeRefValue(root); rf {

		if rv == "" {
			return nil, nil, fmt.Errorf("reference at line %d, column %d is empty, it cannot be resolved",
				root.Line, root.Column), ctx
		}

		// run through everything and return as soon as we find a match.
		// this operates as fast as possible as ever
		collections := generateIndexCollection(idx)
		var found map[string]*index.Reference
		for _, collection := range collections {
			found = collection()
			if found != nil && found[rv] != nil {
				// if this is a ref node, we need to keep diving
				// until we hit something that isn't a ref.
				if jh, _, _ := utils.IsNodeRefValue(found[rv].Node); jh {
					// if this node is circular, stop drop and roll.
					if !IsCircular(found[rv].Node, idx) && found[rv].Node != root {
						return LocateRefNodeWithContext(ctx, found[rv].Node, idx)
					} else {

						crr := GetCircularReferenceResult(found[rv].Node, idx)
						jp := ""
						if crr != nil {
							jp = crr.GenerateJourneyPath()
						}
						return found[rv].Node, idx, fmt.Errorf("circular reference '%s' found during lookup at line "+
							"%d, column %d, It cannot be resolved",
							jp,
							found[rv].Node.Line,
							found[rv].Node.Column), ctx
					}
				}
				return utils.NodeAlias(found[rv].Node), idx, nil, ctx
			}
		}

		// Obtain the absolute filepath/URL of the spec in which we are trying to
		// resolve the reference value [rv] from. It's either available from the
		// index or passed down through context.
		specPath := idx.GetSpecAbsolutePath()
		if ctx.Value(index.CurrentPathKey) != nil {
			specPath = ctx.Value(index.CurrentPathKey).(string)
		}

		// explodedRefValue contains both the path to the file containing the
		// reference value at index 0 and the path within that file to a specific
		// sub-schema, should it exist, at index 1.
		explodedRefValue := strings.Split(rv, "#")
		if len(explodedRefValue) == 2 {
			// The ref points to a component within either this file or another file.
			if !strings.HasPrefix(explodedRefValue[0], "http") {
				// The ref is not an absolute URL.
				if !filepath.IsAbs(explodedRefValue[0]) {
					// The ref is not an absolute local file path.
					if strings.HasPrefix(specPath, "http") {
						// The schema containing the ref is itself a remote file.
						u, _ := url.Parse(specPath)
						// p is the directory the referenced file is expected to be in.
						p := ""
						if u.Path != "" && explodedRefValue[0] != "" {
							// We are using the path of the resolved URL from the rolodex to
							// obtain the "folder" or base of the file URL.
							p = filepath.Dir(u.Path)
						}
						if p != "" && explodedRefValue[0] != "" {
							// We are resolving the relative URL against the absolute URL of
							// the spec containing the reference.
							u.Path = utils.ReplaceWindowsDriveWithLinuxPath(filepath.Join(p, explodedRefValue[0]))
						}
						u.Fragment = ""
						// Turn the reference value [rv] into the absolute filepath/URL we
						// resolved.
						rv = fmt.Sprintf("%s#%s", u.String(), explodedRefValue[1])
					} else {
						// The schema containing the ref is a local file or doesn't have an
						// absolute URL.
						if specPath != "" {
							// We have _some_ path for the schema containing the reference.
							var abs string
							if explodedRefValue[0] == "" {
								// Reference is made within the schema file, so we are using the
								// same absolute local filepath.
								abs = specPath
							} else {
								// break off any fragments from the spec path
								sp := strings.Split(specPath, "#")
								// Create a clean (absolute?) path to the file containing the
								// referenced value.
								abs, _ = filepath.Abs(filepath.Join(filepath.Dir(sp[0]), explodedRefValue[0]))
							}
							rv = fmt.Sprintf("%s#%s", abs, explodedRefValue[1])
						} else {
							// We don't have a path for the schema we are trying to resolve
							// relative references from. This likely happens when the schema
							// is the root schema, i.e. the file given to libopenapi as entry.
							//

							// check for a config BaseURL and use that if it exists.
							if idx.GetConfig().BaseURL != nil {
								u := *idx.GetConfig().BaseURL
								p := ""
								if u.Path != "" {
									p = u.Path
								}

								u.Path = utils.ReplaceWindowsDriveWithLinuxPath(filepath.Join(p, explodedRefValue[0]))
								rv = fmt.Sprintf("%s#%s", u.String(), explodedRefValue[1])
							}
						}
					}
				}
			}
		} else {
			if !strings.HasPrefix(explodedRefValue[0], "http") {
				if !filepath.IsAbs(explodedRefValue[0]) {
					if strings.HasPrefix(specPath, "http") {
						u, _ := url.Parse(specPath)
						p := filepath.Dir(u.Path)
						abs, _ := filepath.Abs(filepath.Join(p, rv))
						u.Path = utils.ReplaceWindowsDriveWithLinuxPath(abs)
						rv = u.String()

					} else {
						if specPath != "" {

							abs, _ := filepath.Abs(filepath.Join(filepath.Dir(specPath), rv))
							rv = abs

						} else {
							// check for a config baseURL and use that if it exists.
							if idx.GetConfig().BaseURL != nil {
								u := *idx.GetConfig().BaseURL
								abs, _ := filepath.Abs(filepath.Join(u.Path, rv))
								u.Path = utils.ReplaceWindowsDriveWithLinuxPath(abs)
								rv = u.String()
							}
						}
					}
				}
			}
		}

		foundRef, fIdx, newCtx := idx.SearchIndexForReferenceWithContext(ctx, rv)
		if foundRef != nil {
			return utils.NodeAlias(foundRef.Node), fIdx, nil, newCtx
		}

		// let's try something else to find our references.

		// cant be found? last resort is to try a path lookup
		_, friendly := utils.ConvertComponentIdIntoFriendlyPathSearch(rv)
		if friendly != "" {
			path, err := jsonpath.NewPath(friendly, jsonpathconfig.WithPropertyNameExtension())
			if err == nil {
				nodes := path.Query(idx.GetRootNode())
				if len(nodes) > 0 {
					return utils.NodeAlias(nodes[0]), idx, nil, ctx
				}
			}
		}
		return nil, idx, fmt.Errorf("reference '%s' at line %d, column %d was not found",
			rv, root.Line, root.Column), ctx
	}
	return nil, idx, nil, ctx
}

// LocateRefNode will perform a complete lookup for a $ref node. This function searches the entire index for
// the reference being supplied. If there is a match found, the reference *yaml.Node is returned.
func LocateRefNode(root *yaml.Node, idx *index.SpecIndex) (*yaml.Node, *index.SpecIndex, error) {
	r, i, e, _ := LocateRefNodeWithContext(context.Background(), root, idx)
	return r, i, e
}

// ExtractObjectRaw will extract a typed Buildable[N] object from a root yaml.Node. The 'raw' aspect is
// that there is no NodeReference wrapper around the result returned, just the raw object.
func ExtractObjectRaw[T Buildable[N], N any](ctx context.Context, key, root *yaml.Node, idx *index.SpecIndex) (T, error, bool, string) {
	var circError error
	var isReference bool
	var referenceValue string
	var refNode *yaml.Node
	root = utils.NodeAlias(root)
	if h, _, rv := utils.IsNodeRefValue(root); h {
		ref, fIdx, err, nCtx := LocateRefNodeWithContext(ctx, root, idx)
		if ref != nil {
			refNode = root
			root = ref
			isReference = true
			referenceValue = rv
			idx = fIdx
			ctx = nCtx
			if err != nil {
				circError = err
			}
		} else {
			if err != nil {
				return nil, fmt.Errorf("object extraction failed: %s", err.Error()), isReference, referenceValue
			}
		}
	}
	var n T = new(N)
	err := BuildModel(root, n)
	if err != nil {
		return n, err, isReference, referenceValue
	}
	err = n.Build(ctx, key, root, idx)
	if err != nil {
		return n, err, isReference, referenceValue
	}

	// if this is a reference, keep track of the reference in the value
	if isReference {
		SetReference(n, referenceValue, refNode)
	}

	// do we want to throw an error as well if circular error reporting is on?
	if circError != nil && !idx.AllowCircularReferenceResolving() {
		return n, circError, isReference, referenceValue
	}
	return n, nil, isReference, referenceValue
}

// ExtractObject will extract a typed Buildable[N] object from a root yaml.Node. The result is wrapped in a
// NodeReference[T] that contains the key node found and value node found when looking up the reference.
func ExtractObject[T Buildable[N], N any](ctx context.Context, label string, root *yaml.Node, idx *index.SpecIndex) (NodeReference[T], error) {
	var ln, vn *yaml.Node
	var circError error
	var isReference bool
	var referenceValue string
	var refNode *yaml.Node
	root = utils.NodeAlias(root)
	if rf, rl, refVal := utils.IsNodeRefValue(root); rf {
		ref, fIdx, err, nCtx := LocateRefNodeWithContext(ctx, root, idx)
		if ref != nil {
			refNode = root
			vn = ref
			ln = rl
			isReference = true
			referenceValue = refVal
			idx = fIdx
			ctx = nCtx
			if err != nil {
				circError = err
			}
		} else {
			if err != nil {
				return NodeReference[T]{}, fmt.Errorf("object extraction failed: %s", err.Error())
			}
		}
	} else {
		_, ln, vn = utils.FindKeyNodeFull(label, root.Content)
		if vn != nil {
			if h, _, rVal := utils.IsNodeRefValue(vn); h {
				ref, fIdx, lerr, nCtx := LocateRefNodeWithContext(ctx, vn, idx)
				if ref != nil {
					refNode = vn
					vn = ref
					if fIdx != nil {
						idx = fIdx
					}
					ctx = nCtx
					isReference = true
					referenceValue = rVal
					if lerr != nil {
						circError = lerr
					}
				} else {
					if lerr != nil {
						return NodeReference[T]{}, fmt.Errorf("object extraction failed: %s", lerr.Error())
					}
				}
			}
		}
	}
	var n T = new(N)
	err := BuildModel(vn, n)
	if err != nil {
		return NodeReference[T]{}, err
	}
	if ln == nil {
		return NodeReference[T]{}, nil
	}
	err = n.Build(ctx, ln, vn, idx)
	if err != nil {
		return NodeReference[T]{}, err
	}

	// if this is a reference, keep track of the reference in the value
	if isReference {
		SetReference(n, referenceValue, refNode)
	}

	res := NodeReference[T]{
		Value:     n,
		KeyNode:   ln,
		ValueNode: vn,
	}
	res.SetReference(referenceValue, refNode)

	// do we want to throw an error as well if circular error reporting is on?
	if circError != nil && !idx.AllowCircularReferenceResolving() {
		return res, circError
	}
	return res, nil
}

func SetReference(obj any, ref string, refNode *yaml.Node) {
	if obj == nil {
		return
	}

	if r, ok := obj.(SetReferencer); ok {
		r.SetReference(ref, refNode)
	}
}

// ExtractArray will extract a slice of []ValueReference[T] from a root yaml.Node that is defined as a sequence.
// Used when the value being extracted is an array.
func ExtractArray[T Buildable[N], N any](ctx context.Context, label string, root *yaml.Node, idx *index.SpecIndex) ([]ValueReference[T],
	*yaml.Node, *yaml.Node, error,
) {
	var ln, vn *yaml.Node
	var circError error
	root = utils.NodeAlias(root)
	isRef := false
	if rf, rl, _ := utils.IsNodeRefValue(root); rf {
		ref, fIdx, err, nCtx := LocateRefEnd(ctx, root, idx, 0)
		if ref != nil {
			isRef = true
			vn = ref
			ln = rl
			idx = fIdx
			ctx = nCtx
			if err != nil {
				circError = err
			}
		} else {
			return []ValueReference[T]{}, nil, nil, fmt.Errorf("array build failed: reference cannot be found: %s",
				root.Content[1].Value)
		}
	} else {
		_, ln, vn = utils.FindKeyNodeFullTop(label, root.Content)
		if vn != nil {
			if h, _, _ := utils.IsNodeRefValue(vn); h {
				ref, fIdx, err, nCtx := LocateRefEnd(ctx, vn, idx, 0)
				if ref != nil {
					isRef = true
					vn = ref
					idx = fIdx
					ctx = nCtx
					if err != nil {
						circError = err
					}
				} else {
					if err != nil {
						return []ValueReference[T]{}, nil, nil,
							fmt.Errorf("array build failed: reference cannot be found: %s",
								err.Error())
					}
				}
			}
		}
	}

	var items []ValueReference[T]
	if vn != nil && ln != nil {
		if !utils.IsNodeArray(vn) {

			if !isRef {
				return []ValueReference[T]{}, nil, nil,
					fmt.Errorf("array build failed, input is not an array, line %d, column %d", vn.Line, vn.Column)
			}
			// if this was pulled from a ref, but it's not a sequence, check the label and see if anything comes out,
			// and then check that is a sequence, if not, fail it.
			_, _, fvn := utils.FindKeyNodeFullTop(label, vn.Content)
			if fvn != nil {
				if !utils.IsNodeArray(vn) {
					return []ValueReference[T]{}, nil, nil,
						fmt.Errorf("array build failed, input is not an array, line %d, column %d", vn.Line, vn.Column)
				}
			}
		}
		for _, node := range vn.Content {
			localReferenceValue := ""
			foundCtx := ctx
			foundIndex := idx

			var refNode *yaml.Node

			if rf, _, rv := utils.IsNodeRefValue(node); rf {
				refg, fIdx, err, nCtx := LocateRefEnd(ctx, node, idx, 0)
				if refg != nil {
					refNode = node
					node = refg
					localReferenceValue = rv
					foundIndex = fIdx
					foundCtx = nCtx
					if err != nil {
						circError = err
					}
				} else {
					if err != nil {
						return []ValueReference[T]{}, nil, nil, fmt.Errorf("array build failed: reference cannot be found: %s",
							err.Error())
					}
				}
			}
			var n T = new(N)
			err := BuildModel(node, n)
			if err != nil {
				return []ValueReference[T]{}, ln, vn, err
			}
			berr := n.Build(foundCtx, ln, node, foundIndex)
			if berr != nil {
				return nil, ln, vn, berr
			}

			if localReferenceValue != "" {
				SetReference(n, localReferenceValue, refNode)
			}

			v := ValueReference[T]{
				Value:     n,
				ValueNode: node,
			}
			v.SetReference(localReferenceValue, refNode)

			items = append(items, v)
		}
	}
	// include circular errors?
	if circError != nil && !idx.AllowCircularReferenceResolving() {
		return items, ln, vn, circError
	}
	return items, ln, vn, nil
}

// ExtractMapNoLookupExtensions will extract a map of KeyReference and ValueReference from a root yaml.Node. The 'NoLookup' part
// refers to the fact that there is no key supplied as part of the extraction, there  is no lookup performed and the
// root yaml.Node pointer is used directly. Pass a true bit to includeExtensions to include extension keys in the map.
//
// This is useful when the node to be extracted, is already known and does not require a search.
func ExtractMapNoLookupExtensions[PT Buildable[N], N any](
	ctx context.Context,
	root *yaml.Node,
	idx *index.SpecIndex,
	includeExtensions bool,
) (*orderedmap.Map[KeyReference[string], ValueReference[PT]], error) {
	valueMap := orderedmap.New[KeyReference[string], ValueReference[PT]]()
	var circError error
	if utils.IsNodeMap(root) {
		var currentKey *yaml.Node
		skip := false
		rlen := len(root.Content)

		for i := 0; i < rlen; i++ {
			node := root.Content[i]
			if !includeExtensions {
				if strings.HasPrefix(strings.ToLower(node.Value), "x-") {
					skip = true
					continue
				}
			}
			if skip {
				skip = false
				continue
			}
			if i%2 == 0 {
				currentKey = node
				continue
			}

			if currentKey.Tag == "!!merge" && currentKey.Value == "<<" {
				root.Content = append(root.Content, utils.NodeAlias(node).Content...)
				rlen = len(root.Content)
				currentKey = nil
				continue
			}
			node = utils.NodeAlias(node)

			foundIndex := idx
			foundContext := ctx

			var isReference bool
			var referenceValue string
			var refNode *yaml.Node
			// if value is a reference, we have to look it up in the index!
			if h, _, rv := utils.IsNodeRefValue(node); h {
				ref, fIdx, err, nCtx := LocateRefNodeWithContext(foundContext, node, foundIndex)
				if ref != nil {
					refNode = node
					node = ref
					isReference = true
					referenceValue = rv
					if fIdx != nil {
						foundIndex = fIdx
					}
					foundContext = nCtx
					if err != nil {
						circError = err
					}
				} else {
					if err != nil {
						return nil, fmt.Errorf("map build failed: reference cannot be found: %s", err.Error())
					}
				}
			}
			var n PT = new(N)
			err := BuildModel(node, n)
			if err != nil {
				return nil, err
			}
			berr := n.Build(foundContext, currentKey, node, foundIndex)
			if berr != nil {
				return nil, berr
			}
			if isReference {
				SetReference(n, referenceValue, refNode)
			}
			if currentKey != nil {
				v := ValueReference[PT]{
					Value:     n,
					ValueNode: node,
				}
				v.SetReference(referenceValue, refNode)

				valueMap.Set(
					KeyReference[string]{
						Value:   currentKey.Value,
						KeyNode: currentKey,
					},
					v,
				)
			}
		}
	}
	if circError != nil && !idx.AllowCircularReferenceResolving() {
		return valueMap, circError
	}
	return valueMap, nil
}

// ExtractMapNoLookup will extract a map of KeyReference and ValueReference from a root yaml.Node. The 'NoLookup' part
// refers to the fact that there is no key supplied as part of the extraction, there  is no lookup performed and the
// root yaml.Node pointer is used directly.
//
// This is useful when the node to be extracted, is already known and does not require a search.
func ExtractMapNoLookup[PT Buildable[N], N any](
	ctx context.Context,
	root *yaml.Node,
	idx *index.SpecIndex,
) (*orderedmap.Map[KeyReference[string], ValueReference[PT]], error) {
	return ExtractMapNoLookupExtensions[PT, N](ctx, root, idx, false)
}

type mappingResult[T any] struct {
	k KeyReference[string]
	v ValueReference[T]
}

type buildInput struct {
	label *yaml.Node
	value *yaml.Node
}

// ExtractMapExtensions will extract a map of KeyReference and ValueReference from a root yaml.Node. The 'label' is
// used to locate the node to be extracted from the root node supplied. Supply a bit to decide if extensions should
// be included or not. required in some use cases.
//
// The second return value is the yaml.Node found for the 'label' and the third return value is the yaml.Node
// found for the value extracted from the label node.
func ExtractMapExtensions[PT Buildable[N], N any](
	ctx context.Context,
	label string,
	root *yaml.Node,
	idx *index.SpecIndex,
	extensions bool,
) (*orderedmap.Map[KeyReference[string], ValueReference[PT]], *yaml.Node, *yaml.Node, error) {
	var labelNode, valueNode *yaml.Node
	var circError error
	root = utils.NodeAlias(root)
	foundIndex := idx
	foundContext := ctx
	if rf, rl, _ := utils.IsNodeRefValue(root); rf {
		// locate reference in index.
		ref, fIdx, err, fCtx := LocateRefNodeWithContext(ctx, root, idx)
		if ref != nil {
			valueNode = ref
			labelNode = rl
			foundContext = fCtx
			foundIndex = fIdx
			if err != nil {
				circError = err
			}
		} else {
			return nil, labelNode, valueNode, fmt.Errorf("map build failed: reference cannot be found: %s",
				root.Content[1].Value)
		}
	} else {
		_, labelNode, valueNode = utils.FindKeyNodeFull(label, root.Content)
		valueNode = utils.NodeAlias(valueNode)
		if valueNode != nil {
			if h, _, _ := utils.IsNodeRefValue(valueNode); h {
				ref, fIdx, err, nCtx := LocateRefNodeWithContext(ctx, valueNode, idx)
				if ref != nil {
					valueNode = ref
					foundIndex = fIdx
					foundContext = nCtx
					if err != nil {
						circError = err
					}
				} else {
					if err != nil {
						return nil, labelNode, valueNode, fmt.Errorf("map build failed: reference cannot be found: %s",
							err.Error())
					}
				}
			}
		}
	}
	if valueNode != nil {
		valueMap := orderedmap.New[KeyReference[string], ValueReference[PT]]()

		in := make(chan buildInput)
		out := make(chan mappingResult[PT])
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2) // input and output goroutines.

		// TranslatePipeline input.
		go func() {
			defer func() {
				close(in)
				wg.Done()
			}()
			var currentLabelNode *yaml.Node
			for i, en := range valueNode.Content {
				if !extensions {
					if strings.HasPrefix(en.Value, "x-") {
						continue // yo, don't pay any attention to extensions, not here anyway.
					}
				}
				if currentLabelNode == nil && i%2 != 0 {
					continue // we need a label node first, and we don't have one because of extensions.
				}

				en = utils.NodeAlias(en)
				if i%2 == 0 {
					currentLabelNode = en
					continue
				}

				select {
				case in <- buildInput{
					label: currentLabelNode,
					value: en,
				}:
				case <-done:
					return
				}
			}
		}()

		// TranslatePipeline output.
		go func() {
			for {
				result, ok := <-out
				if !ok {
					break
				}
				valueMap.Set(result.k, result.v)
			}
			close(done)
			wg.Done()
		}()

		startIdx := foundIndex
		startCtx := foundContext

		translateFunc := func(input buildInput) (mappingResult[PT], error) {
			en := input.value

			sCtx := startCtx
			sIdx := startIdx

			var refNode *yaml.Node
			var referenceValue string
			// check our valueNode isn't a reference still.
			if h, _, refVal := utils.IsNodeRefValue(en); h {
				ref, fIdx, err, nCtx := LocateRefNodeWithContext(sCtx, en, sIdx)
				if ref != nil {
					refNode = en
					en = ref
					referenceValue = refVal
					if fIdx != nil {
						sIdx = fIdx
					}
					sCtx = nCtx
					if err != nil {
						circError = err
					}
				} else {
					if err != nil {
						return mappingResult[PT]{}, fmt.Errorf("flat map build failed: reference cannot be found: %s",
							err.Error())
					}
				}
			}

			var n PT = new(N)
			en = utils.NodeAlias(en)
			_ = BuildModel(en, n)
			err := n.Build(sCtx, input.label, en, sIdx)
			if err != nil {
				return mappingResult[PT]{}, err
			}

			if referenceValue != "" {
				SetReference(n, referenceValue, refNode)
			}

			v := ValueReference[PT]{
				Value:     n,
				ValueNode: en,
			}
			v.SetReference(referenceValue, refNode)

			return mappingResult[PT]{
				k: KeyReference[string]{
					KeyNode: input.label,
					Value:   input.label.Value,
				},
				v: v,
			}, nil
		}

		err := datamodel.TranslatePipeline[buildInput, mappingResult[PT]](in, out, translateFunc)
		wg.Wait()
		if err != nil {
			return nil, labelNode, valueNode, err
		}
		if circError != nil && !idx.AllowCircularReferenceResolving() {
			return valueMap, labelNode, valueNode, circError
		}
		return valueMap, labelNode, valueNode, nil
	}
	return nil, labelNode, valueNode, nil
}

// ExtractMap will extract a map of KeyReference and ValueReference from a root yaml.Node. The 'label' is
// used to locate the node to be extracted from the root node supplied.
//
// The second return value is the yaml.Node found for the 'label' and the third return value is the yaml.Node
// found for the value extracted from the label node.
func ExtractMap[PT Buildable[N], N any](
	ctx context.Context,
	label string,
	root *yaml.Node,
	idx *index.SpecIndex,
) (*orderedmap.Map[KeyReference[string], ValueReference[PT]], *yaml.Node, *yaml.Node, error) {
	return ExtractMapExtensions[PT, N](ctx, label, root, idx, false)
}

// ExtractExtensions will extract any 'x-' prefixed key nodes from a root node into a map. Requirements have been pre-cast:
//
// Maps
//
//	*orderedmap.Map[string, *yaml.Node] for maps
//
// Slices
//
//	[]interface{}
//
// int, float, bool, string
//
//	int64, float64, bool, string
func ExtractExtensions(root *yaml.Node) *orderedmap.Map[KeyReference[string], ValueReference[*yaml.Node]] {
	root = utils.NodeAlias(root)
	if root == nil {
		return nil
	}
	extensions := utils.FindExtensionNodes(root.Content)
	extensionMap := orderedmap.New[KeyReference[string], ValueReference[*yaml.Node]]()
	for _, ext := range extensions {
		extensionMap.Set(KeyReference[string]{
			Value:   ext.Key.Value,
			KeyNode: ext.Key,
		}, ValueReference[*yaml.Node]{Value: ext.Value, ValueNode: ext.Value})
	}
	return extensionMap
}

// AreEqual returns true if two Hashable objects are equal or not.
func AreEqual(l, r Hashable) bool {
	if l == nil || r == nil {
		return false
	}
	vol := reflect.ValueOf(l)
	vor := reflect.ValueOf(r)

	if vol.Kind() != reflect.Struct && vor.Kind() != reflect.Struct {
		if vol.IsNil() || vor.IsNil() {
			return false
		}
	}
	return l.Hash() == r.Hash()
}

// GenerateHashString will generate a SHA256 hash of any object passed in. If the object is Hashable
// then the underlying Hash() method will be called. Optimized to avoid excessive allocations and
// uses caching to eliminate redundant calculations.
func GenerateHashString(v any) string {
	if v == nil {
		return ""
	}

	// Try cache first using the pointer as key for non-primitives
	// However, skip caching for types with mutable hash state like SchemaProxy
	val := reflect.ValueOf(v)
	shouldCache := true
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		// Check if this is a type that has mutable hash state or complex comparison logic
		typeName := val.Type().String()
		if typeName == "*base.SchemaProxy" || typeName == "*base.Schema" {
			shouldCache = false
		}

		if shouldCache {
			cacheKey := val.Pointer()
			if cached, ok := hashCache.Load(cacheKey); ok {
				return cached.(string)
			}
		}
	}

	var hashStr string

	if h, ok := v.(Hashable); ok {
		if h != nil {
			// Use hex.EncodeToString which is more efficient than fmt.Sprintf
			hash := h.Hash()
			hashStr = hex.EncodeToString(hash[:])
		}
	} else if n, ok := v.(*yaml.Node); ok {
		// Fast path for common YAML node types to avoid marshaling
		hashStr = hashYamlNodeFast(n)
	} else {
		// Primitive types
		// if we get here, we're a primitive, check if we're a pointer and de-point
		if val.Kind() == reflect.Ptr {
			v = val.Elem().Interface()
		}

		// Convert to string efficiently using strconv instead of fmt.Sprintf
		var str string
		switch val := v.(type) {
		case string:
			str = val
		case int:
			str = strconv.Itoa(val)
		case int8:
			str = strconv.FormatInt(int64(val), 10)
		case int16:
			str = strconv.FormatInt(int64(val), 10)
		case int32:
			str = strconv.FormatInt(int64(val), 10)
		case int64:
			str = strconv.FormatInt(val, 10)
		case uint:
			str = strconv.FormatUint(uint64(val), 10)
		case uint8:
			str = strconv.FormatUint(uint64(val), 10)
		case uint16:
			str = strconv.FormatUint(uint64(val), 10)
		case uint32:
			str = strconv.FormatUint(uint64(val), 10)
		case uint64:
			str = strconv.FormatUint(val, 10)
		case float32:
			str = strconv.FormatFloat(float64(val), 'g', -1, 32)
		case float64:
			str = strconv.FormatFloat(val, 'g', -1, 64)
		case bool:
			if val {
				str = "true"
			} else {
				str = "false"
			}
		default:
			str = fmt.Sprint(v)
		}

		// Use hex.EncodeToString which is more efficient than fmt.Sprintf
		hash := sha256.Sum256([]byte(str))
		hashStr = hex.EncodeToString(hash[:])
	}

	// Store in cache if we have a valid pointer and caching is enabled for this type
	if shouldCache && val.Kind() == reflect.Ptr && !val.IsNil() && hashStr != "" {
		cacheKey := val.Pointer()
		hashCache.Store(cacheKey, hashStr)
	}

	return hashStr
}

// hashYamlNodeFast provides fast hashing for YAML nodes without ANY marshaling
func hashYamlNodeFast(n *yaml.Node) string {
	if n == nil {
		return ""
	}

	// Try cache first for complex nodes
	if n.Kind != yaml.ScalarNode {
		cacheKey := uintptr(unsafe.Pointer(n))
		if cached, ok := hashCache.Load(cacheKey); ok {
			return cached.(string)
		}
	}

	// Use a hasher instead of marshaling
	h := sha256.New()
	visited := make(map[*yaml.Node]bool)
	hashNodeTree(h, n, visited)

	// Use hex.EncodeToString which is more efficient than fmt.Sprintf
	result := hex.EncodeToString(h.Sum(nil))

	// Cache complex nodes
	if n.Kind != yaml.ScalarNode {
		cacheKey := uintptr(unsafe.Pointer(n))
		hashCache.Store(cacheKey, result)
	}

	return result
}

// hashNodeTree walks the YAML tree and hashes it without marshaling
func hashNodeTree(h hash.Hash, n *yaml.Node, visited map[*yaml.Node]bool) {
	if n == nil {
		return
	}

	// Prevent circular reference infinite loops
	if visited[n] {
		h.Write([]byte("<<CIRCULAR>>"))
		return
	}
	visited[n] = true

	// Hash node metadata
	h.Write([]byte{byte(n.Kind)})
	h.Write([]byte(n.Tag))
	h.Write([]byte(n.Value))
	if n.Anchor != "" {
		h.Write([]byte(n.Anchor))
	}

	// Hash based on node type
	switch n.Kind {
	case yaml.ScalarNode:
		// Already hashed value above

	case yaml.SequenceNode:
		h.Write([]byte("["))
		for _, child := range n.Content {
			hashNodeTree(h, child, visited)
			h.Write([]byte(","))
		}
		h.Write([]byte("]"))

	case yaml.MappingNode:
		h.Write([]byte("{"))
		// For maps, we need consistent ordering
		// Collect key-value pairs and sort by key hash
		type kvPair struct {
			keyHash   string
			keyNode   *yaml.Node
			valueNode *yaml.Node
		}
		pairs := make([]kvPair, 0, len(n.Content)/2)

		for i := 0; i < len(n.Content); i += 2 {
			if i+1 < len(n.Content) {
				// Hash the key for sorting
				keyH := sha256.New()
				hashNodeTree(keyH, n.Content[i], visited)
				pairs = append(pairs, kvPair{
					keyHash:   fmt.Sprintf("%x", keyH.Sum(nil)),
					keyNode:   n.Content[i],
					valueNode: n.Content[i+1],
				})
			}
		}

		// Sort for consistent hashing
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].keyHash < pairs[j].keyHash
		})

		// Hash in sorted order
		for _, pair := range pairs {
			hashNodeTree(h, pair.keyNode, visited)
			h.Write([]byte(":"))
			hashNodeTree(h, pair.valueNode, visited)
			h.Write([]byte(","))
		}
		h.Write([]byte("}"))

	case yaml.DocumentNode:
		h.Write([]byte("DOC["))
		for _, child := range n.Content {
			hashNodeTree(h, child, visited)
		}
		h.Write([]byte("]"))

	case yaml.AliasNode:
		h.Write([]byte("ALIAS["))
		if n.Alias != nil {
			hashNodeTree(h, n.Alias, visited)
		}
		h.Write([]byte("]"))
	}
}

// CompareYAMLNodes compares two YAML nodes for equality without marshaling to YAML.
// This reuses the hashNodeTree logic to generate consistent hashes for comparison,
// avoiding the expensive yaml.Marshal operations that cause massive allocations.
func CompareYAMLNodes(left, right *yaml.Node) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}

	// Use the existing hashNodeTree function to generate consistent hashes
	leftHash := sha256.New()
	rightHash := sha256.New()

	leftVisited := make(map[*yaml.Node]bool)
	rightVisited := make(map[*yaml.Node]bool)

	hashNodeTree(leftHash, left, leftVisited)
	hashNodeTree(rightHash, right, rightVisited)

	leftSum := leftHash.Sum(nil)
	rightSum := rightHash.Sum(nil)

	// Compare the hash bytes directly
	return bytes.Equal(leftSum, rightSum)
}

// YAMLNodeToBytes converts a YAML node to bytes in a more efficient way than yaml.Marshal
// This function should be used when you actually need the marshaled bytes (like for JSON conversion)
// rather than just comparing nodes (use CompareYAMLNodes for that)
func YAMLNodeToBytes(n *yaml.Node) ([]byte, error) {
	if n == nil {
		return nil, nil
	}
	// For now, we still use yaml.Marshal for cases that actually need the bytes
	// This can be optimized further in the future with a custom serializer
	return yaml.Marshal(n)
}

// HashYAMLNodeSlice creates a hash for a slice of YAML nodes efficiently
// This replaces the pattern of yaml.Marshal + sha256 that's used in example comparisons
func HashYAMLNodeSlice(nodes []*yaml.Node) string {
	if len(nodes) == 0 {
		return ""
	}

	h := sha256.New()
	visited := make(map[*yaml.Node]bool)

	for _, node := range nodes {
		hashNodeTree(h, node, visited)
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

// AppendMapHashes will append all the hashes of a map to a slice of strings.
// Optimized to avoid creating sorted copies on every call.
func AppendMapHashes[v any](a []string, m *orderedmap.Map[KeyReference[string], ValueReference[v]]) []string {
	if m == nil {
		return a
	}

	// Pre-allocate slice for better performance when we know the size
	if cap(a)-len(a) < m.Len() {
		newA := make([]string, len(a), len(a)+m.Len())
		copy(newA, a)
		a = newA
	}

	// Collect entries and sort them by key for consistent hashing
	// This is more efficient than orderedmap.SortAlpha() which creates a full copy
	type entry struct {
		key   string
		value v
	}
	entries := make([]entry, 0, m.Len())

	for k, v := range m.FromOldest() {
		entries = append(entries, entry{
			key:   k.Value,
			value: v.Value,
		})
	}

	// Sort entries by key for consistent hash ordering
	// Use a simple insertion sort for small maps, quicksort for larger ones
	if len(entries) <= 10 {
		// Insertion sort for small maps
		for i := 1; i < len(entries); i++ {
			key := entries[i]
			j := i - 1
			for j >= 0 && entries[j].key > key.key {
				entries[j+1] = entries[j]
				j--
			}
			entries[j+1] = key
		}
	} else {
		// Use Go's built-in sort for larger maps
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].key < entries[j].key
		})
	}

	// For small maps, avoid string builder overhead and use direct string concatenation
	if len(entries) <= 5 {
		for _, entry := range entries {
			hashStr := entry.key + "-" + GenerateHashString(entry.value)
			a = append(a, hashStr)
		}
	} else {
		// Use string builder for larger maps with pre-allocated capacity
		sb := GetStringBuilder()
		defer PutStringBuilder(sb)

		for _, entry := range entries {
			sb.Reset()
			// Pre-size for this specific entry to avoid growth
			expectedLen := len(entry.key) + 64 + 1 // key + hash + separator
			sb.Grow(expectedLen)
			sb.WriteString(entry.key)
			sb.WriteByte('-')
			sb.WriteString(GenerateHashString(entry.value))
			a = append(a, sb.String())
		}
	}

	return a
}

func ValueToString(v any) string {
	if n, ok := v.(*yaml.Node); ok {
		// For simple scalar nodes, return the value directly
		if n.Kind == yaml.ScalarNode {
			return n.Value
		}
		// For complex nodes, still need to marshal for string representation
		b, _ := yaml.Marshal(n)
		return string(b)
	}

	return fmt.Sprint(v)
}

// LocateRefEnd will perform a complete lookup for a $ref node. This function searches the entire index for
// the reference being supplied. If there is a match found, the reference *yaml.Node is returned.
// the function operates recursively and will keep iterating through references until it finds a non-reference
// node.
func LocateRefEnd(ctx context.Context, root *yaml.Node, idx *index.SpecIndex, depth int) (*yaml.Node, *index.SpecIndex, error, context.Context) {
	depth++
	if depth > 100 {
		return nil, nil, fmt.Errorf("reference resolution depth exceeded, possible circular reference"), ctx
	}
	ref, fIdx, err, nCtx := LocateRefNodeWithContext(ctx, root, idx)
	if err != nil {
		return ref, fIdx, err, nCtx
	}
	if rf, _, _ := utils.IsNodeRefValue(ref); rf {
		return LocateRefEnd(nCtx, ref, fIdx, depth)
	} else {
		return ref, fIdx, err, nCtx
	}
}

// FromReferenceMap will convert a *orderedmap.Map[KeyReference[K], ValueReference[V]] to a *orderedmap.Map[K, V]
func FromReferenceMap[K comparable, V any](refMap *orderedmap.Map[KeyReference[K], ValueReference[V]]) *orderedmap.Map[K, V] {
	om := orderedmap.New[K, V]()
	for k, v := range refMap.FromOldest() {
		om.Set(k.Value, v.Value)
	}
	return om
}

// FromReferenceMapWithFunc will convert a *orderedmap.Map[KeyReference[K], ValueReference[V]] to a *orderedmap.Map[K, VOut] using a transform function
func FromReferenceMapWithFunc[K comparable, V any, VOut any](refMap *orderedmap.Map[KeyReference[K], ValueReference[V]], transform func(v V) VOut) *orderedmap.Map[K, VOut] {
	om := orderedmap.New[K, VOut]()
	for k, v := range refMap.FromOldest() {
		om.Set(k.Value, transform(v.Value))
	}
	return om
}
