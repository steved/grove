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

package pod

import (
	"testing"

	apicommon "github.com/ai-dynamo/grove/operator/api/common"
	apicommonconstants "github.com/ai-dynamo/grove/operator/api/common/constants"
	componentutils "github.com/ai-dynamo/grove/operator/internal/controller/common/component/utils"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestConfigureSpreadTopologyPod(t *testing.T) {
	baseAnnotations := map[string]string{
		apicommonconstants.AnnotationTopologySpreadTopologyKey:       "node.kubernetes.io/fabric-pod",
		apicommonconstants.AnnotationTopologySpreadAllDomains:        "fabric-a,fabric-b",
		apicommonconstants.AnnotationTopologySpreadActiveDomains:     "fabric-a",
		apicommonconstants.AnnotationTopologySpreadReplicasPerDomain: "2",
	}

	t.Run("placeholder pod uses pause image and preserves resource requests", func(t *testing.T) {
		t.Setenv(envVarTopologySpreadPlaceholderImage, "example.com/pause:test")
		annotations := cloneStringMap(baseAnnotations)
		annotations[apicommonconstants.AnnotationTopologySpreadPhase] = componentutils.TopologySpreadPhasePlaceholder
		pod := &corev1.Pod{Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "init", Image: "busybox"}},
			Containers: []corev1.Container{{
				Name:  "worker",
				Image: "workload:test",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2")},
				},
				ReadinessProbe: &corev1.Probe{},
			}},
		}}

		err := configureSpreadTopologyPod(annotations, "pcs-0-cyborg", pod, 3)
		require.NoError(t, err)
		require.Equal(t, "true", pod.Labels[apicommon.LabelTopologySpreadPlaceholder])
		require.Equal(t, "fabric-b", pod.Labels[apicommon.LabelTopologySpreadDomain])
		require.Empty(t, pod.Spec.InitContainers)
		require.Equal(t, "example.com/pause:test", pod.Spec.Containers[0].Image)
		require.Nil(t, pod.Spec.Containers[0].ReadinessProbe)
		require.Equal(t, resource.MustParse("2"), pod.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU])
		requireSpreadAffinity(t, pod, "node.kubernetes.io/fabric-pod", "fabric-b")
	})

	t.Run("actual pod keeps workload container and is pinned to active domain", func(t *testing.T) {
		annotations := cloneStringMap(baseAnnotations)
		annotations[apicommonconstants.AnnotationTopologySpreadPhase] = componentutils.TopologySpreadPhaseActual
		pod := &corev1.Pod{Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "init", Image: "busybox"}},
			Containers:     []corev1.Container{{Name: "worker", Image: "workload:test"}},
		}}

		err := configureSpreadTopologyPod(annotations, "pcs-0-cyborg", pod, 1)
		require.NoError(t, err)
		require.Equal(t, "false", pod.Labels[apicommon.LabelTopologySpreadPlaceholder])
		require.Equal(t, "fabric-a", pod.Labels[apicommon.LabelTopologySpreadDomain])
		require.Len(t, pod.Spec.InitContainers, 1)
		require.Equal(t, "workload:test", pod.Spec.Containers[0].Image)
		requireSpreadAffinity(t, pod, "node.kubernetes.io/fabric-pod", "fabric-a")
	})
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func requireSpreadAffinity(t *testing.T, pod *corev1.Pod, key, value string) {
	t.Helper()
	require.NotNil(t, pod.Spec.Affinity)
	require.NotNil(t, pod.Spec.Affinity.NodeAffinity)
	required := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	require.NotNil(t, required)
	require.Len(t, required.NodeSelectorTerms, 1)
	require.Contains(t, required.NodeSelectorTerms[0].MatchExpressions, corev1.NodeSelectorRequirement{
		Key:      key,
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{value},
	})
}
