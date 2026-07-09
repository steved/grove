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

package topology

import (
	"context"
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/controller/topologyresolver"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestAssociatedScheduledDomainsEmptyOnlyWhenAllSourcesScaledToZero(t *testing.T) {
	pcs := &grovecorev1alpha1.PodCliqueSet{ObjectMeta: metav1.ObjectMeta{Name: "workload", Namespace: "default"}}
	target := &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: "workload-0-target", Namespace: "default"}}
	affinity := &grovecorev1alpha1.TopologyAffinity{CliqueNames: []string{"source-a", "source-b"}}
	level := grovecorev1alpha1.TopologyLevel{Domain: "block", Key: "network.example.com/block"}

	tests := []struct {
		name      string
		replicas  []int32
		wantReady bool
	}{
		{name: "all sources scaled to zero", replicas: []int32{0, 0}, wantReady: true},
		{name: "non-zero source has no domain evidence", replicas: []int32{0, 1}, wantReady: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objects := make([]client.Object, 0, len(test.replicas))
			for i, replicas := range test.replicas {
				objects = append(objects, &grovecorev1alpha1.PodClique{
					ObjectMeta: metav1.ObjectMeta{
						Name:      apicommon.GeneratePodCliqueName(apicommon.ResourceNameReplica{Name: pcs.Name, Replica: 0}, affinity.CliqueNames[i]),
						Namespace: pcs.Namespace,
					},
					Spec:   grovecorev1alpha1.PodCliqueSpec{Replicas: replicas},
					Status: grovecorev1alpha1.PodCliqueStatus{ScheduledReplicas: replicas},
				})
			}
			cl := testutils.SetupFakeClient(objects...)

			domains, ready, err := AssociatedScheduledDomains(context.Background(), cl, topologyresolver.NewReconciler(cl), pcs, target, affinity, level)
			require.NoError(t, err)
			assert.Empty(t, domains)
			assert.Equal(t, test.wantReady, ready)
		})
	}
}

func TestAssociatedPodDomainsFromAllocatedDevices(t *testing.T) {
	const attribute = resourcev1.QualifiedName("network.example.com/block")
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, resourcev1.AddToScheme(scheme))

	level := grovecorev1alpha1.TopologyLevel{
		Domain: "block",
		Key:    "network.example.com/block",
		ResourceSliceAttributes: []grovecorev1alpha1.ResourceSliceAttributeReference{{
			Driver: "devices.example.com",
			Name:   resourcev1.FullyQualifiedName(attribute),
		}},
	}

	tests := []struct {
		name             string
		deviceDomains    []string
		selectableDomain []string
		wantDomains      []string
		wantError        string
	}{
		{name: "one domain", deviceDomains: []string{"b"}, selectableDomain: []string{"a", "b"}, wantDomains: []string{"b"}},
		{name: "multiple domains", deviceDomains: []string{"a", "b"}, selectableDomain: []string{"a", "b"}, wantError: "resolves to multiple topology domains"},
		{name: "unselectable domain", deviceDomains: []string{"b"}, selectableDomain: []string{"a"}, wantError: "is not selectable by Node label"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "default"},
				Spec: corev1.PodSpec{
					NodeName: "node-a",
					ResourceClaims: []corev1.PodResourceClaim{{
						Name:              "device",
						ResourceClaimName: ptr.To("source-device"),
					}},
				},
			}
			claim := &resourcev1.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "source-device", Namespace: "default"},
				Status:     resourcev1.ResourceClaimStatus{Allocation: &resourcev1.AllocationResult{}},
			}
			slice := &resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice"},
				Spec: resourcev1.ResourceSliceSpec{
					Driver: "devices.example.com",
					Pool:   resourcev1.ResourcePool{Name: "pool", Generation: 1, ResourceSliceCount: 1},
				},
			}
			for _, domain := range test.deviceDomains {
				name := "device-" + domain
				claim.Status.Allocation.Devices.Results = append(claim.Status.Allocation.Devices.Results, resourcev1.DeviceRequestAllocationResult{
					Driver: "devices.example.com",
					Pool:   "pool",
					Device: name,
				})
				slice.Spec.Devices = append(slice.Spec.Devices, resourcev1.Device{
					Name:       name,
					Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{attribute: {StringValue: ptr.To(domain)}},
				})
			}
			objects := []runtime.Object{claim, slice}
			for _, domain := range test.selectableDomain {
				objects = append(objects, &corev1.Node{ObjectMeta: metav1.ObjectMeta{
					Name:   "node-" + domain,
					Labels: map[string]string{level.Key: domain},
				}})
			}
			cl := clientfake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(objects...).Build()

			domains, err := associatedPodDomains(context.Background(), cl, topologyresolver.NewReconciler(cl), pod, level)
			if test.wantError != "" {
				require.ErrorContains(t, err, test.wantError)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantDomains, domains)
		})
	}
}

func TestAssociatedPodDomainsWithoutResourceSliceEvidenceUsesNode(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, resourcev1.AddToScheme(scheme))

	const key = "network.example.com/block"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			ResourceClaims: []corev1.PodResourceClaim{{
				Name:              "device",
				ResourceClaimName: ptr.To("unallocated-claim"),
			}},
		},
	}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   "node-a",
		Labels: map[string]string{key: "a"},
	}}
	cl := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()

	domains, err := associatedPodDomains(context.Background(), cl, topologyresolver.NewReconciler(cl), pod, grovecorev1alpha1.TopologyLevel{Domain: "block", Key: key})
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, domains)
}
