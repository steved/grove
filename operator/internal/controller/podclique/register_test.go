// /*
// Copyright 2025 The Grove Authors.
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

package podclique

import (
	"context"
	"testing"

	"github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	"github.com/ai-dynamo/grove/operator/internal/expect"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// TestControllerConstants tests the controller constants
func TestControllerConstants(t *testing.T) {
	// Verifies that controller name is set correctly
	assert.Equal(t, "podclique-controller", controllerName)
}

// TestPodPredicate_Delete tests the pod predicate's Delete path for the scenario:
// when a managed pod (e.g. pending) is manually deleted, the informer sees a Delete event before the next reconcile.
// The predicate must call ObserveDeletions so the pod's UID is removed from create expectations (uidsToAdd),
// allowing the controller to recreate the pod on the next reconcile instead of treating it as "informer slow".
func TestPodPredicate_Delete(t *testing.T) {
	const ns, pclqName, podName = "default", "pclq-1", "pclq-1-0"
	pclqKey, err := expect.ControlleeKeyFunc(&grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: pclqName}})
	require.NoError(t, err)

	t.Run("managed pod with PodClique owner: ObserveDeletions removes UID from create expectations so pod can be recreated", func(t *testing.T) {
		store := expect.NewExpectationsStore()
		podUID := types.UID("pod-deleted-manually")
		require.NoError(t, store.ExpectCreations(logr.Discard(), pclqKey, podUID))

		createExpectations := store.GetCreateExpectations(pclqKey)
		require.Contains(t, createExpectations, podUID, "setup: create expectation should contain pod UID")

		r := &Reconciler{expectationsStore: store}
		pred := r.podPredicate()
		pod := testutils.NewPodBuilder(podName, ns).
			WithOwner(pclqName).
			WithLabels(map[string]string{common.LabelManagedByKey: common.LabelManagedByValue}).
			Build()
		pod.UID = podUID

		funcs, ok := pred.(predicate.Funcs)
		require.True(t, ok, "predicate must be predicate.Funcs")
		result := funcs.DeleteFunc(event.DeleteEvent{Object: pod})

		createExpectationsAfter := store.GetCreateExpectations(pclqKey)
		assert.NotContains(t, createExpectationsAfter, podUID,
			"ObserveDeletions should remove the deleted pod UID from uidsToAdd so next reconcile can recreate the pod")
		assert.True(t, result, "predicate should allow the event so the handler enqueues reconcile")
	})
}

func TestTopologyAffinitySourcePodPredicate(t *testing.T) {
	const namespace, sourcePCLQName = "default", "workload-0-workers-0-source"
	managedPod := func(nodeName string) *corev1.Pod {
		pod := testutils.NewPodBuilder("source-0", namespace).
			WithOwner(sourcePCLQName).
			WithLabels(map[string]string{common.LabelManagedByKey: common.LabelManagedByValue}).
			Build()
		pod.Spec.NodeName = nodeName
		return pod
	}

	pred, ok := topologyAffinitySourcePodPredicate().(predicate.Funcs)
	require.True(t, ok, "predicate must be predicate.Funcs")

	t.Run("node assignment enqueues despite pod generation change", func(t *testing.T) {
		oldPod := managedPod("")
		oldPod.Generation = 1
		newPod := oldPod.DeepCopy()
		newPod.Generation = 2
		newPod.Spec.NodeName = "worker-a"

		assert.True(t, pred.UpdateFunc(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}))
	})

	t.Run("unrelated status update does not enqueue", func(t *testing.T) {
		oldPod := managedPod("worker-a")
		newPod := oldPod.DeepCopy()
		newPod.Status.Phase = corev1.PodRunning

		assert.False(t, pred.UpdateFunc(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}))
	})

	t.Run("pod creation does not duplicate primary startup reconciliation", func(t *testing.T) {
		assert.False(t, pred.CreateFunc(event.CreateEvent{Object: managedPod("worker-a")}))
	})

	t.Run("bound pod deletion enqueues", func(t *testing.T) {
		assert.True(t, pred.DeleteFunc(event.DeleteEvent{Object: managedPod("worker-a")}))
		assert.False(t, pred.DeleteFunc(event.DeleteEvent{Object: managedPod("")}))
	})

	t.Run("unmanaged pod does not enqueue", func(t *testing.T) {
		oldPod := managedPod("")
		delete(oldPod.Labels, common.LabelManagedByKey)
		newPod := oldPod.DeepCopy()
		newPod.Spec.NodeName = "worker-a"
		assert.False(t, pred.UpdateFunc(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}))
	})
}

func TestMapTopologyAffinitySourcePodToDependentPCLQs(t *testing.T) {
	const (
		pcsName   = "workload"
		pcsgName  = "workload-0-workers"
		namespace = "default"
	)
	replicas := int32(2)
	pcs := testutils.NewPodCliqueSetBuilder(pcsName, namespace, types.UID("pcs-uid")).
		WithReplicas(2).
		WithPodCliqueTemplateSpec(testutils.NewBasicPodCliqueTemplateSpec("source")).
		WithPodCliqueTemplateSpec(testutils.NewBasicPodCliqueTemplateSpec("target")).
		WithPodCliqueScalingGroupConfig(grovecorev1alpha1.PodCliqueScalingGroupConfig{
			Name:         "workers",
			CliqueNames:  []string{"source", "target"},
			Replicas:     &replicas,
			MinAvailable: &replicas,
		}).
		Build()

	newPCLQ := func(pcsReplica, pcsgReplica int, cliqueName string) *grovecorev1alpha1.PodClique {
		name := common.GeneratePodCliqueName(
			common.ResourceNameReplica{
				Name:    common.GeneratePodCliqueScalingGroupName(common.ResourceNameReplica{Name: pcsName, Replica: pcsReplica}, "workers"),
				Replica: pcsgReplica,
			},
			cliqueName,
		)
		return testutils.NewPCSGPodCliqueBuilder(name, namespace, pcsName,
			common.GeneratePodCliqueScalingGroupName(common.ResourceNameReplica{Name: pcsName, Replica: pcsReplica}, "workers"),
			pcsReplica, pcsgReplica).
			Build()
	}

	source0 := newPCLQ(0, 0, "source")
	source1 := newPCLQ(0, 1, "source")
	target0 := newPCLQ(0, 0, "target")
	target1 := newPCLQ(0, 1, "target")
	targetInOtherPCSReplica := newPCLQ(1, 0, "target")
	for _, target := range []*grovecorev1alpha1.PodClique{target0, target1, targetInOtherPCSReplica} {
		target.Spec.Affinity = &grovecorev1alpha1.PodCliqueAffinity{
			TopologyAffinity: &grovecorev1alpha1.TopologyAffinity{CliqueNames: []string{"source"}},
		}
	}

	cl := testutils.SetupFakeClient(pcs, source0, source1, target0, target1, targetInOtherPCSReplica)
	r := &Reconciler{client: cl}
	tests := []struct {
		name             string
		source           *grovecorev1alpha1.PodClique
		pcsgReplicaIndex string
		target           *grovecorev1alpha1.PodClique
	}{
		{name: "replica zero", source: source0, pcsgReplicaIndex: "0", target: target0},
		{name: "replica one", source: source1, pcsgReplicaIndex: "1", target: target1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pod := testutils.NewPodBuilder(test.source.Name+"-pod", namespace).
				WithOwner(test.source.Name).
				WithLabels(map[string]string{
					common.LabelManagedByKey:                      common.LabelManagedByValue,
					common.LabelPartOfKey:                         pcsName,
					common.LabelPodClique:                         test.source.Name,
					common.LabelPodCliqueSetReplicaIndex:          "0",
					common.LabelPodCliqueScalingGroup:             pcsgName,
					common.LabelPodCliqueScalingGroupReplicaIndex: test.pcsgReplicaIndex,
				}).
				Build()
			pod.Spec.NodeName = "worker-a"

			requests := r.mapTopologyAffinitySourcePodToDependentPCLQs()(context.Background(), pod)
			assert.Equal(t, []reconcile.Request{
				{NamespacedName: types.NamespacedName{Namespace: namespace, Name: test.target.Name}},
			}, requests)
		})
	}
}

// TestPodCliqueSetPredicateCurrentlyUpdatingReplicaChanges verifies that the PodCliqueSet
// watch predicate enqueues PodClique reconciles when the replica currently being rolled out
// changes.
func TestPodCliqueSetPredicateCurrentlyUpdatingReplicaChanges(t *testing.T) {
	pred, ok := podCliqueSetPredicate().(predicate.Funcs)
	require.True(t, ok, "predicate must be predicate.Funcs")

	tests := []struct {
		name        string
		oldProgress *grovecorev1alpha1.PodCliqueSetUpdateProgress
		newProgress *grovecorev1alpha1.PodCliqueSetUpdateProgress
		want        bool
	}{
		{
			name: "currently updating starts",
			newProgress: &grovecorev1alpha1.PodCliqueSetUpdateProgress{
				CurrentlyUpdating: []grovecorev1alpha1.PodCliqueSetReplicaUpdateProgress{{ReplicaIndex: 0}},
			},
			want: true,
		},
		{
			name: "currently updating clears",
			oldProgress: &grovecorev1alpha1.PodCliqueSetUpdateProgress{
				CurrentlyUpdating: []grovecorev1alpha1.PodCliqueSetReplicaUpdateProgress{{ReplicaIndex: 0}},
			},
			newProgress: &grovecorev1alpha1.PodCliqueSetUpdateProgress{},
			want:        true,
		},
		{
			name: "currently updating moves",
			oldProgress: &grovecorev1alpha1.PodCliqueSetUpdateProgress{
				CurrentlyUpdating: []grovecorev1alpha1.PodCliqueSetReplicaUpdateProgress{{ReplicaIndex: 0}},
			},
			newProgress: &grovecorev1alpha1.PodCliqueSetUpdateProgress{
				CurrentlyUpdating: []grovecorev1alpha1.PodCliqueSetReplicaUpdateProgress{{ReplicaIndex: 1}},
			},
			want: true,
		},
		{
			name: "currently updating unchanged",
			oldProgress: &grovecorev1alpha1.PodCliqueSetUpdateProgress{
				CurrentlyUpdating: []grovecorev1alpha1.PodCliqueSetReplicaUpdateProgress{{ReplicaIndex: 0}},
			},
			newProgress: &grovecorev1alpha1.PodCliqueSetUpdateProgress{
				CurrentlyUpdating: []grovecorev1alpha1.PodCliqueSetReplicaUpdateProgress{{ReplicaIndex: 0}},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.UpdateFunc(event.UpdateEvent{
				ObjectOld: &grovecorev1alpha1.PodCliqueSet{Status: grovecorev1alpha1.PodCliqueSetStatus{CurrentGenerationHash: ptr.To("generation"), UpdateProgress: tt.oldProgress}},
				ObjectNew: &grovecorev1alpha1.PodCliqueSet{Status: grovecorev1alpha1.PodCliqueSetStatus{CurrentGenerationHash: ptr.To("generation"), UpdateProgress: tt.newProgress}},
			})
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPodCliqueScalingGroupPredicateGenerationStatusChanges verifies that the
// PodCliqueScalingGroup watch predicate triggers PodClique reconciles when the PCSG's view
// of the PodCliqueSet generation changes during a rolling update.
func TestPodCliqueScalingGroupPredicateGenerationStatusChanges(t *testing.T) {
	pred, ok := podCliqueScalingGroupPredicate().(predicate.Funcs)
	require.True(t, ok, "predicate must be predicate.Funcs")

	tests := []struct {
		name    string
		oldPCSG *grovecorev1alpha1.PodCliqueScalingGroup
		newPCSG *grovecorev1alpha1.PodCliqueScalingGroup
		want    bool
	}{
		{
			name: "current generation changes",
			oldPCSG: &grovecorev1alpha1.PodCliqueScalingGroup{Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
				CurrentPodCliqueSetGenerationHash: ptr.To("old-generation"),
			}},
			newPCSG: &grovecorev1alpha1.PodCliqueScalingGroup{Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
				CurrentPodCliqueSetGenerationHash: ptr.To("new-generation"),
			}},
			want: true,
		},
		{
			name: "equal current generation values with different pointers do not enqueue",
			oldPCSG: &grovecorev1alpha1.PodCliqueScalingGroup{Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
				CurrentPodCliqueSetGenerationHash: ptr.To("generation"),
			}},
			newPCSG: &grovecorev1alpha1.PodCliqueScalingGroup{Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
				CurrentPodCliqueSetGenerationHash: ptr.To("generation"),
			}},
			want: false,
		},
		{
			name: "update target generation changes",
			oldPCSG: &grovecorev1alpha1.PodCliqueScalingGroup{Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
				UpdateProgress: &grovecorev1alpha1.PodCliqueScalingGroupUpdateProgress{PodCliqueSetGenerationHash: "old-generation"},
			}},
			newPCSG: &grovecorev1alpha1.PodCliqueScalingGroup{Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
				UpdateProgress: &grovecorev1alpha1.PodCliqueScalingGroupUpdateProgress{PodCliqueSetGenerationHash: "new-generation"},
			}},
			want: true,
		},
		{
			name: "generation status unchanged",
			oldPCSG: &grovecorev1alpha1.PodCliqueScalingGroup{Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
				CurrentPodCliqueSetGenerationHash: ptr.To("generation"),
				UpdateProgress:                    &grovecorev1alpha1.PodCliqueScalingGroupUpdateProgress{PodCliqueSetGenerationHash: "generation"},
			}},
			newPCSG: &grovecorev1alpha1.PodCliqueScalingGroup{Status: grovecorev1alpha1.PodCliqueScalingGroupStatus{
				CurrentPodCliqueSetGenerationHash: ptr.To("generation"),
				UpdateProgress:                    &grovecorev1alpha1.PodCliqueScalingGroupUpdateProgress{PodCliqueSetGenerationHash: "generation"},
			}},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.UpdateFunc(event.UpdateEvent{ObjectOld: tt.oldPCSG, ObjectNew: tt.newPCSG})
			assert.Equal(t, tt.want, got)
		})
	}
}

// Test_isMarkedForDeletion tests if a deletion timestamp is set on the pod
func Test_isMarkedForDeletion(t *testing.T) {
	now := ptr.To(metav1.Now())
	tests := []struct {
		name        string
		updateEvent event.UpdateEvent
		want        bool
	}{
		{
			name: "deletion timestamp set on the pod in the update",
			updateEvent: event.UpdateEvent{
				ObjectOld: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						DeletionTimestamp: nil,
					},
				},
				ObjectNew: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						DeletionTimestamp: now,
					},
				},
			},
			want: true,
		},
		{
			name: "deletion timestamp not set on the pod in the update",
			updateEvent: event.UpdateEvent{
				ObjectOld: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						DeletionTimestamp: nil,
					},
				},
				ObjectNew: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						DeletionTimestamp: nil,
					},
				},
			},
			want: false,
		},
		{
			name: "deletion timestamp was already set on the pod before the update",
			updateEvent: event.UpdateEvent{
				ObjectOld: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						DeletionTimestamp: now,
					},
				},
				ObjectNew: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						DeletionTimestamp: now,
					},
				},
			},
			want: false,
		},
		{
			name: "objects are not pods (type cast fails)",
			updateEvent: event.UpdateEvent{
				ObjectOld: &corev1.ConfigMap{},
				ObjectNew: &corev1.ConfigMap{},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, isMarkedForDeletion(tt.updateEvent), "isMarkedForDeletionChanged(%v)", tt.updateEvent)
		})
	}
}
