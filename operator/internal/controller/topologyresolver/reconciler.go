// /*
// Copyright 2026 The Grove Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// */

package topologyresolver

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	nodeListGVK = corev1.SchemeGroupVersion.WithKind("NodeList")
)

type attributeKey struct {
	driver string
	name   resourcev1.FullyQualifiedName
}

type poolKey struct {
	driver string
	name   string
}

type deviceKey struct {
	driver string
	pool   string
	name   string
}

type resourceSliceSnapshot struct {
	devices map[attributeKey]map[deviceKey]string
	errs    map[attributeKey]error
}

// Reconciler maintains one manager-scoped view of Node labels and ResourceSlices.
type Reconciler struct {
	client client.Client

	rebuildMu sync.Mutex
	mu        sync.RWMutex
	nodes     map[string]map[string]string
	values    map[string]sets.Set[string]
	refs      map[attributeKey]struct{}
	devices   map[attributeKey]map[deviceKey]string
	sliceErrs map[attributeKey]error
}

// NewReconciler creates a shared topology resolver.
func NewReconciler(cl client.Client) *Reconciler {
	return &Reconciler{
		client:    cl,
		nodes:     make(map[string]map[string]string),
		values:    make(map[string]sets.Set[string]),
		refs:      make(map[attributeKey]struct{}),
		devices:   make(map[attributeKey]map[deviceKey]string),
		sliceErrs: make(map[attributeKey]error),
	}
}

// Reconcile refreshes all registered topology projections.
func (r *Reconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	return ctrl.Result{}, r.rebuild(ctx)
}

// NodeValues returns the unique non-empty values for a Node label.
func (r *Reconciler) NodeValues(ctx context.Context, labelKey string) ([]string, error) {
	if err := r.ensureNodeKey(ctx, labelKey); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return sets.List(r.values[labelKey]), nil
}

// NodeValue returns one Node's non-empty value for a registered label.
func (r *Reconciler) NodeValue(ctx context.Context, labelKey, nodeName string) (string, error) {
	if nodeName == "" {
		return "", fmt.Errorf("node name must not be empty")
	}
	if err := r.ensureNodeKey(ctx, labelKey); err != nil {
		return "", err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if value := r.nodes[labelKey][nodeName]; value != "" {
		return value, nil
	}
	return "", fmt.Errorf("node %q does not have label %q", nodeName, labelKey)
}

// ResourceSliceDeviceDomains resolves allocated device identities through the
// current complete ResourceSlice generations. The boolean reports whether any
// allocated device belongs to a configured topology-bearing driver.
func (r *Reconciler) ResourceSliceDeviceDomains(ctx context.Context, refs []grovecorev1alpha1.ResourceSliceAttributeReference, allocated []resourcev1.DeviceRequestAllocationResult) ([]string, bool, error) {
	if err := r.ensureResourceSliceRefs(ctx, refs); err != nil {
		return nil, false, err
	}

	refsByDriver := make(map[string]attributeKey, len(refs))
	for _, ref := range refs {
		refsByDriver[ref.Driver] = attributeKey{driver: ref.Driver, name: ref.Name}
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	domains := sets.New[string]()
	topologyBearing := false
	for _, device := range allocated {
		ref, ok := refsByDriver[device.Driver]
		if !ok {
			continue
		}
		topologyBearing = true
		if err := r.sliceErrs[ref]; err != nil {
			return nil, true, err
		}
		key := deviceKey{driver: device.Driver, pool: device.Pool, name: device.Device}
		domain, ok := r.devices[ref][key]
		if !ok {
			return nil, true, fmt.Errorf("allocated device %q/%q/%q does not publish string topology attribute %q in the current complete ResourceSlice generation", device.Driver, device.Pool, device.Device, ref.name)
		}
		domains.Insert(domain)
	}
	return sets.List(domains), topologyBearing, nil
}

func (r *Reconciler) ensureResourceSliceRefs(ctx context.Context, refs []grovecorev1alpha1.ResourceSliceAttributeReference) error {
	if len(refs) == 0 {
		return fmt.Errorf("resourceSliceAttributes must not be empty")
	}

	registered := make([]attributeKey, 0, len(refs))
	r.mu.Lock()
	for _, ref := range refs {
		key := attributeKey{driver: ref.Driver, name: ref.Name}
		if _, ok := r.refs[key]; !ok {
			r.refs[key] = struct{}{}
			registered = append(registered, key)
		}
	}
	r.mu.Unlock()
	if len(registered) > 0 {
		if err := r.rebuild(ctx); err != nil {
			r.mu.Lock()
			for _, key := range registered {
				delete(r.refs, key)
				delete(r.devices, key)
				delete(r.sliceErrs, key)
			}
			r.mu.Unlock()
			return fmt.Errorf("failed to list ResourceSlices: %w", err)
		}
	}
	return nil
}

func (r *Reconciler) ensureNodeKey(ctx context.Context, labelKey string) error {
	if labelKey == "" {
		return fmt.Errorf("label key must not be empty")
	}
	r.mu.RLock()
	_, ok := r.values[labelKey]
	r.mu.RUnlock()
	if ok {
		return nil
	}

	r.mu.Lock()
	if _, ok = r.values[labelKey]; !ok {
		r.values[labelKey] = sets.New[string]()
		r.nodes[labelKey] = make(map[string]string)
	}
	r.mu.Unlock()
	if err := r.rebuild(ctx); err != nil {
		r.mu.Lock()
		delete(r.values, labelKey)
		delete(r.nodes, labelKey)
		r.mu.Unlock()
		return fmt.Errorf("failed to list Node metadata for label %q: %w", labelKey, err)
	}
	return nil
}

func (r *Reconciler) rebuild(ctx context.Context) error {
	r.rebuildMu.Lock()
	defer r.rebuildMu.Unlock()

	r.mu.RLock()
	labels := slices.Collect(maps.Keys(r.values))
	refs := slices.Collect(maps.Keys(r.refs))
	r.mu.RUnlock()

	if len(labels) > 0 {
		nodes, values, err := r.rebuildNodes(ctx, labels)
		if err != nil {
			return err
		}
		r.mu.Lock()
		for _, label := range labels {
			r.nodes[label] = nodes[label]
			r.values[label] = values[label]
		}
		r.mu.Unlock()
	}

	if len(refs) > 0 {
		snapshot, err := r.rebuildResourceSlices(ctx, refs)
		if err != nil {
			return err
		}
		r.mu.Lock()
		for _, ref := range refs {
			r.devices[ref] = snapshot.devices[ref]
			r.sliceErrs[ref] = snapshot.errs[ref]
		}
		r.mu.Unlock()
	}
	return nil
}

func (r *Reconciler) rebuildNodes(ctx context.Context, labels []string) (map[string]map[string]string, map[string]sets.Set[string], error) {
	list := &metav1.PartialObjectMetadataList{}
	list.SetGroupVersionKind(nodeListGVK)
	if err := r.client.List(ctx, list); err != nil {
		return nil, nil, err
	}

	nodes := make(map[string]map[string]string, len(labels))
	values := make(map[string]sets.Set[string], len(labels))
	for _, label := range labels {
		nodes[label] = make(map[string]string)
		values[label] = sets.New[string]()
	}
	for _, node := range list.Items {
		for _, label := range labels {
			if value := node.Labels[label]; value != "" {
				nodes[label][node.Name] = value
				values[label].Insert(value)
			}
		}
	}
	return nodes, values, nil
}

func (r *Reconciler) rebuildResourceSlices(ctx context.Context, refs []attributeKey) (resourceSliceSnapshot, error) {
	list := &resourcev1.ResourceSliceList{}
	if err := r.client.List(ctx, list); err != nil {
		return resourceSliceSnapshot{}, err
	}

	pools := make(map[poolKey][]resourcev1.ResourceSlice)
	for _, slice := range list.Items {
		key := poolKey{driver: slice.Spec.Driver, name: slice.Spec.Pool.Name}
		pools[key] = append(pools[key], slice)
	}

	devices := make(map[attributeKey]map[deviceKey]string, len(refs))
	errs := make(map[attributeKey]error, len(refs))
	for _, ref := range refs {
		devices[ref] = make(map[deviceKey]string)
	}
	for key, slicesForPool := range pools {
		generation := slicesForPool[0].Spec.Pool.Generation
		for i := 1; i < len(slicesForPool); i++ {
			generation = max(generation, slicesForPool[i].Spec.Pool.Generation)
		}
		current := slices.DeleteFunc(slicesForPool, func(slice resourcev1.ResourceSlice) bool {
			return slice.Spec.Pool.Generation != generation
		})
		if len(current) == 0 {
			continue
		}
		expected := current[0].Spec.Pool.ResourceSliceCount
		if int64(len(current)) != expected {
			for _, ref := range refs {
				if ref.driver == key.driver {
					errs[ref] = fmt.Errorf("ResourceSlice pool %q for driver %q generation %d is incomplete: expected %d slices, found %d", key.name, key.driver, generation, expected, len(current))
				}
			}
			continue
		}
		for _, slice := range current {
			for _, device := range slice.Spec.Devices {
				key := deviceKey{driver: slice.Spec.Driver, pool: slice.Spec.Pool.Name, name: device.Name}
				for _, ref := range refs {
					if ref.driver != key.driver {
						continue
					}
					attribute, ok := device.Attributes[resourcev1.QualifiedName(ref.name)]
					if ok && attribute.StringValue != nil && *attribute.StringValue != "" {
						devices[ref][key] = *attribute.StringValue
					}
				}
			}
		}
	}
	return resourceSliceSnapshot{devices: devices, errs: errs}, nil
}
