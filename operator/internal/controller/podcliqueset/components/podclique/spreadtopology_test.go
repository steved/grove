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

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apicommonconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	grovecorev1alpha1 "github.com/ai-dynamo/grove/operator/api/core/v1alpha1"
	groveclientscheme "github.com/ai-dynamo/grove/operator/internal/client"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"
	testutils "github.com/ai-dynamo/grove/operator/test/utils"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const testSpreadTopologyKey = "node.kubernetes.io/fabric-pod"

func TestApplySpreadTopologyState(t *testing.T) {
	tests := []struct {
		name              string
		objects           []client.Object
		wantPhase         string
		wantReplicas      int32
		wantMinAvailable  int32
		wantAllDomains    string
		wantActiveDomains string
	}{
		{
			name:             "placeholder phase creates replicas for every discovered topology domain",
			objects:          spreadTopologyBaseObjects(),
			wantPhase:        componentutils.TopologySpreadPhasePlaceholder,
			wantReplicas:     4,
			wantMinAvailable: 2,
			wantAllDomains:   "fabric-a,fabric-b",
		},
		{
			name: "actual phase creates replicas only in ready dependency domains",
			objects: append(spreadTopologyBaseObjects(),
				readyParentPodClique(),
				readyParentPod("parent-a-0", "node-a"),
			),
			wantPhase:         componentutils.TopologySpreadPhaseActual,
			wantReplicas:      2,
			wantMinAvailable:  1,
			wantAllDomains:    "fabric-a,fabric-b",
			wantActiveDomains: "fabric-a",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cl := fake.NewClientBuilder().WithScheme(groveclientscheme.Scheme).WithObjects(tc.objects...).Build()
			operator := &_resource{client: cl, scheme: groveclientscheme.Scheme}

			pcs := spreadTopologyPCS()
			pclq := &grovecorev1alpha1.PodClique{ObjectMeta: metav1.ObjectMeta{Name: "spread-pcs-0-cyborg", Namespace: "default"}}
			err := operator.buildResource(context.Background(), logr.Discard(), pclq, pcs, 0, false)
			require.NoError(t, err)

			require.Equal(t, tc.wantReplicas, pclq.Spec.Replicas)
			require.NotNil(t, pclq.Spec.MinAvailable)
			require.Equal(t, tc.wantMinAvailable, *pclq.Spec.MinAvailable)
			require.Equal(t, tc.wantPhase, pclq.Annotations[apicommonconstants.AnnotationTopologySpreadPhase])
			require.Equal(t, tc.wantAllDomains, pclq.Annotations[apicommonconstants.AnnotationTopologySpreadAllDomains])
			require.Equal(t, tc.wantActiveDomains, pclq.Annotations[apicommonconstants.AnnotationTopologySpreadActiveDomains])
		})
	}
}

func spreadTopologyPCS() *grovecorev1alpha1.PodCliqueSet {
	startupType := grovecorev1alpha1.CliqueStartupTypeExplicit
	return testutils.NewPodCliqueSetBuilder("spread-pcs", "default", uuid.NewUUID()).
		WithReplicas(1).
		WithCliqueStartupType(&startupType).
		WithPodCliqueTemplateSpec(testutils.NewPodCliqueTemplateSpecBuilder("lpu").
			WithReplicas(1).
			WithMinAvailable(1).
			Build()).
		WithPodCliqueTemplateSpec(testutils.NewPodCliqueTemplateSpecBuilder("cyborg").
			WithReplicas(2).
			WithMinAvailable(1).
			WithStartsAfter([]string{"lpu"}).
			WithTopologyConstraint(&grovecorev1alpha1.TopologyConstraint{
				TopologyName: "fabric",
				PackDomain:   grovecorev1alpha1.TopologyDomainBlock,
				Spread:       true,
			}).
			Build()).
		Build()
}

func spreadTopologyBaseObjects() []client.Object {
	return []client.Object{
		&grovecorev1alpha1.ClusterTopology{
			ObjectMeta: metav1.ObjectMeta{Name: "fabric"},
			Spec: grovecorev1alpha1.ClusterTopologySpec{
				Levels: []grovecorev1alpha1.TopologyLevel{
					{Domain: grovecorev1alpha1.TopologyDomainBlock, Key: testSpreadTopologyKey},
				},
			},
		},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a", Labels: map[string]string{testSpreadTopologyKey: "fabric-a"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b", Labels: map[string]string{testSpreadTopologyKey: "fabric-b"}}},
	}
}

func readyParentPodClique() *grovecorev1alpha1.PodClique {
	return &grovecorev1alpha1.PodClique{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "spread-pcs-0-lpu",
			Namespace: "default",
			UID:       types.UID("parent-pclq"),
			Labels: map[string]string{
				apicommon.LabelPartOfKey:                "spread-pcs",
				apicommon.LabelPodCliqueSetReplicaIndex: "0",
			},
		},
		Spec: grovecorev1alpha1.PodCliqueSpec{
			Replicas:     1,
			MinAvailable: ptr.To[int32](1),
		},
	}
}

func readyParentPod(name, nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels: map[string]string{
				apicommon.LabelPodClique: "spread-pcs-0-lpu",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         grovecorev1alpha1.SchemeGroupVersion.String(),
				Kind:               "PodClique",
				Name:               "spread-pcs-0-lpu",
				UID:                types.UID("parent-pclq"),
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			}},
		},
		Spec: corev1.PodSpec{NodeName: nodeName},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}},
	}
}
