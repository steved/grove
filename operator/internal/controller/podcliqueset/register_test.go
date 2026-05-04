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

package podcliqueset

import (
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestMapClusterTopologyToPodCliqueSets(t *testing.T) {
	makePCS := func(namespace, name string, mutate func(*grovecorev1alpha1.PodCliqueSet)) *grovecorev1alpha1.PodCliqueSet {
		pcs := &grovecorev1alpha1.PodCliqueSet{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: grovecorev1alpha1.PodCliqueSetSpec{
				Replicas: 1,
				Template: grovecorev1alpha1.PodCliqueSetTemplateSpec{},
			},
		}
		if mutate != nil {
			mutate(pcs)
		}
		return pcs
	}

	ct := &grovecorev1alpha1.ClusterTopology{ObjectMeta: metav1.ObjectMeta{Name: "selected-topology"}}
	pcsA := makePCS("default", "pcs-a", func(pcs *grovecorev1alpha1.PodCliqueSet) {
		pcs.Spec.Template.TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{
			TopologyName: "selected-topology",
			PackDomain:   grovecorev1alpha1.TopologyDomainRack,
		}
	})
	pcsB := makePCS("team-b", "pcs-b", func(pcs *grovecorev1alpha1.PodCliqueSet) {
		pcs.Spec.Template.Cliques = []*grovecorev1alpha1.PodCliqueTemplateSpec{
			{
				Name: "worker",
				TopologyConstraint: &grovecorev1alpha1.TopologyConstraint{
					TopologyName: "selected-topology",
					PackDomain:   grovecorev1alpha1.TopologyDomainHost,
				},
				Spec: grovecorev1alpha1.PodCliqueSpec{Replicas: 1},
			},
		}
	})
	pcsOther := makePCS("default", "pcs-other", func(pcs *grovecorev1alpha1.PodCliqueSet) {
		pcs.Spec.Template.TopologyConstraint = &grovecorev1alpha1.TopologyConstraint{
			TopologyName: "other-topology",
			PackDomain:   grovecorev1alpha1.TopologyDomainRack,
		}
	})
	pcsWithoutTopology := makePCS("default", "pcs-no-topology", nil)

	fakeClient := testutils.NewTestClientBuilder().
		WithObjects(ct, pcsA, pcsB, pcsOther, pcsWithoutTopology).
		Build()

	mapFn := mapClusterTopologyToPodCliqueSets(fakeClient)
	requests := mapFn(t.Context(), ct)

	require.Len(t, requests, 2)
	assert.ElementsMatch(t, []reconcile.Request{
		{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pcs-a"}},
		{NamespacedName: types.NamespacedName{Namespace: "team-b", Name: "pcs-b"}},
	}, requests)
}

func TestMapManagedPodToPodCliqueSet(t *testing.T) {
	pod := managedPod("lpu-0", "default", "spread-demo")

	requests := mapManagedPodToPodCliqueSet()(t.Context(), pod)

	require.Len(t, requests, 1)
	assert.Equal(t, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "spread-demo"}}, requests[0])
}

func TestManagedPodPlacementPredicate(t *testing.T) {
	pred, ok := managedPodPlacementPredicate().(predicate.Funcs)
	require.True(t, ok)

	oldPod := managedPod("lpu-0", "default", "spread-demo")
	oldPod.Spec.NodeName = "node-a"
	oldPod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}

	t.Run("node placement change enqueues PodCliqueSet", func(t *testing.T) {
		newPod := oldPod.DeepCopy()
		newPod.Spec.NodeName = "node-b"
		assert.True(t, pred.UpdateFunc(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}))
	})

	t.Run("ready condition change enqueues PodCliqueSet", func(t *testing.T) {
		newPod := oldPod.DeepCopy()
		newPod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
		assert.True(t, pred.UpdateFunc(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}))
	})

	t.Run("metadata-only change does not enqueue PodCliqueSet", func(t *testing.T) {
		newPod := oldPod.DeepCopy()
		newPod.Labels["example.com/ignored"] = "changed"
		assert.False(t, pred.UpdateFunc(event.UpdateEvent{ObjectOld: oldPod, ObjectNew: newPod}))
	})
}

func managedPod(name, namespace, pcsName string) *corev1.Pod {
	return testutils.NewPodBuilder(name, namespace).
		WithOwner("spread-demo-0-lpu").
		WithLabels(apicommon.GetDefaultLabelsForPodCliqueSetManagedResources(pcsName)).
		Build()
}
