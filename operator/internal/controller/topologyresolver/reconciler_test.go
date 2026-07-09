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
	"errors"
	"testing"

	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestNodeTopology(t *testing.T) {
	resolver := newTestResolver(t,
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{"topology.grove.io/block": "a"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{"topology.grove.io/block": "b"}}},
	)

	values, err := resolver.NodeValues(context.Background(), "topology.grove.io/block")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, values)

	value, err := resolver.NodeValue(context.Background(), "topology.grove.io/block", "node-b")
	require.NoError(t, err)
	assert.Equal(t, "b", value)
}

func TestResourceSliceDeviceDomainsUsesAllocatedDeviceIdentity(t *testing.T) {
	const attribute = resourcev1.QualifiedName("network.example.com/block")
	resolver := newTestResolver(t, resourceSlice("device-a", 1, 1, "node-a", "a"))

	domains, topologyBearing, err := resolver.ResourceSliceDeviceDomains(
		context.Background(),
		[]grovecorev1alpha1.ResourceSliceAttributeReference{{
			Driver: "devices.example.com",
			Name:   resourcev1.FullyQualifiedName(attribute),
		}},
		[]resourcev1.DeviceRequestAllocationResult{{
			Driver: "devices.example.com",
			Pool:   "pool",
			Device: "device-a",
		}},
	)
	require.NoError(t, err)
	assert.True(t, topologyBearing)
	assert.Equal(t, []string{"a"}, domains)
}

func TestResolverRetriesAfterInitialListFailure(t *testing.T) {
	t.Run("Nodes", func(t *testing.T) {
		const key = "topology.grove.io/block"
		listCalls := 0
		cl := clientfake.NewClientBuilder().
			WithScheme(testScheme(t)).
			WithObjects(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{key: "a"}}}).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					listCalls++
					if listCalls == 1 {
						return errors.New("temporary list failure")
					}
					return cl.List(ctx, list, opts...)
				},
			}).Build()
		resolver := NewReconciler(cl)

		_, err := resolver.NodeValues(context.Background(), key)
		require.Error(t, err)
		values, err := resolver.NodeValues(context.Background(), key)
		require.NoError(t, err)
		assert.Equal(t, []string{"a"}, values)
	})

	t.Run("ResourceSlices", func(t *testing.T) {
		const attribute = resourcev1.FullyQualifiedName("network.example.com/block")
		listCalls := 0
		cl := clientfake.NewClientBuilder().
			WithScheme(testScheme(t)).
			WithObjects(resourceSlice("device-a", 1, 1, "node-a", "a")).
			WithInterceptorFuncs(interceptor.Funcs{
				List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					listCalls++
					if listCalls == 1 {
						return errors.New("temporary list failure")
					}
					return cl.List(ctx, list, opts...)
				},
			}).Build()
		resolver := NewReconciler(cl)
		refs := []grovecorev1alpha1.ResourceSliceAttributeReference{{Driver: "devices.example.com", Name: attribute}}
		devices := []resourcev1.DeviceRequestAllocationResult{{Driver: "devices.example.com", Pool: "pool", Device: "device-a"}}

		_, _, err := resolver.ResourceSliceDeviceDomains(context.Background(), refs, devices)
		require.Error(t, err)
		domains, topologyBearing, err := resolver.ResourceSliceDeviceDomains(context.Background(), refs, devices)
		require.NoError(t, err)
		assert.True(t, topologyBearing)
		assert.Equal(t, []string{"a"}, domains)
	})
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, resourcev1.AddToScheme(scheme))
	return scheme
}

func newTestResolver(t *testing.T, objects ...runtime.Object) *Reconciler {
	t.Helper()
	return NewReconciler(clientfake.NewClientBuilder().WithScheme(testScheme(t)).WithRuntimeObjects(objects...).Build())
}

func resourceSlice(name string, generation, count int64, node, domain string) *resourcev1.ResourceSlice {
	const attribute = resourcev1.QualifiedName("network.example.com/block")
	return &resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: resourcev1.ResourceSliceSpec{
			Driver:   "devices.example.com",
			Pool:     resourcev1.ResourcePool{Name: "pool", Generation: generation, ResourceSliceCount: count},
			NodeName: ptr.To(node),
			Devices: []resourcev1.Device{{
				Name:       name,
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{attribute: {StringValue: ptr.To(domain)}},
			}},
		},
	}
}
